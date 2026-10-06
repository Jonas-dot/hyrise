#include "transaction_manager.hpp"

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

#include "commit_context.hpp"
#include "storage/dependency_validation/dependency_validation_commit_coordinator.hpp"
#include "storage/dependency_validation/dependency_validation_test_hooks.hpp"
#include "transaction_context.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TransactionManager::TransactionManager()
    : _next_transaction_id{INITIAL_TRANSACTION_ID},
      _last_commit_id{INITIAL_COMMIT_ID},
      _last_commit_context{std::make_shared<CommitContext>(INITIAL_COMMIT_ID)},
      _dependency_validation_commit_coordinator{std::make_unique<dv_tree::DependencyValidationCommitCoordinator>()} {}

TransactionManager::~TransactionManager() {
  Assert(_active_snapshot_commit_ids.empty(),
         "Some transactions do not seem to have finished yet as they are still registered as active.");
}

TransactionManager& TransactionManager::operator=(TransactionManager&& transaction_manager) noexcept {
  _next_transaction_id = transaction_manager._next_transaction_id.load();
  _last_commit_id = transaction_manager._last_commit_id.load();
  _last_commit_context = transaction_manager._last_commit_context;
  _active_snapshot_commit_ids = transaction_manager._active_snapshot_commit_ids;
  _dependency_validation_commit_coordinator = std::make_unique<dv_tree::DependencyValidationCommitCoordinator>();
  return *this;
}

CommitID TransactionManager::last_commit_id() const {
  return _last_commit_id;
}

std::shared_ptr<TransactionContext> TransactionManager::new_transaction_context(const AutoCommit auto_commit) {
  CommitID snapshot_commit_id = UNSET_COMMIT_ID;
  {
    const auto lock = std::lock_guard<std::mutex>{_active_snapshot_commit_ids_mutex};
    snapshot_commit_id = _last_commit_id;
    _active_snapshot_commit_ids.insert(snapshot_commit_id);
    _refresh_dependency_validation_snapshot_horizon_locked();
  }

  try {
    return std::shared_ptr<TransactionContext>{
        new TransactionContext{TransactionID{_next_transaction_id++}, snapshot_commit_id, auto_commit, true}};
  } catch (...) {
    _deregister_transaction(snapshot_commit_id);
    throw;
  }
}

void TransactionManager::_register_transaction(const CommitID snapshot_commit_id) {
  const auto lock = std::lock_guard<std::mutex>{_active_snapshot_commit_ids_mutex};
  _active_snapshot_commit_ids.insert(snapshot_commit_id);
  _refresh_dependency_validation_snapshot_horizon_locked();
}

void TransactionManager::_deregister_transaction(const CommitID snapshot_commit_id) {
  const auto lock = std::lock_guard<std::mutex>{_active_snapshot_commit_ids_mutex};
  auto it = std::ranges::find(_active_snapshot_commit_ids, snapshot_commit_id);

  if (it != _active_snapshot_commit_ids.end()) {
    _active_snapshot_commit_ids.erase(it);
  } else {
    Assert(it == _active_snapshot_commit_ids.end(),
           "Could not find snapshot_commit_id in TransactionManager's _active_snapshot_commit_ids. Therefore, the "
           "removal failed and the function should not have been called.");
  }
  _refresh_dependency_validation_snapshot_horizon_locked();
}

std::optional<CommitID> TransactionManager::get_lowest_active_snapshot_commit_id() const {
  const auto lock = std::lock_guard<std::mutex>{_active_snapshot_commit_ids_mutex};

  if (_active_snapshot_commit_ids.empty()) {
    return std::nullopt;
  }

  return std::ranges::min(_active_snapshot_commit_ids);
}

/**
 * Logic of the lock-free algorithm
 *
 * Assume n threads call this method simultaneously. They all enter the main while-loop. Eventually, they reach the
 * point where they try to set the successor of _last_commit_context (pointed to by current_context). Only one of them
 * will succeed and will be able to pass the following if statement. The rest continues with the loop and will now try
 * to get the latest context, which does not have a successor. As long as the thread that succeeded setting the next
 * commit context has not finished updating _last_commit_context, they are stuck in the small while-loop. As soon as it
 * is done, _last_commit_context will point to a commit context with no successor and they will be able to leave this
 * loop.
 */
std::shared_ptr<CommitContext> TransactionManager::_new_commit_context() {
  const auto creation_lock = std::lock_guard<std::mutex>{_commit_context_creation_mutex};
  auto current_context = std::atomic_load(&_last_commit_context);
  while (current_context->has_next()) {
    current_context = std::atomic_load(&_last_commit_context);
  }

  const auto next_context = std::make_shared<CommitContext>(CommitID{current_context->commit_id() + 1});

  // Reserve all coordinator bookkeeping before publishing this context into
  // Hyrise's ordered chain. An allocation failure therefore consumes no CID
  // and cannot leave either chain with a permanent gap.
  _dependency_validation_commit_coordinator->reserve_commit_id(next_context->commit_id());

  const auto successor_set = current_context->try_set_next(next_context);
  Assert(successor_set, "Serialized commit-context creation found an unexpected successor.");
  std::atomic_store(&_last_commit_context, next_context);

  return next_context;
}

std::unique_ptr<dv_tree::DependencyValidationCommit> TransactionManager::_register_dependency_validation_commit(
    const CommitID commit_id, const dv_tree::DependencyValidationWriteSet* write_set) {
  auto commit = _dependency_validation_commit_coordinator->register_commit(commit_id, write_set);
  _refresh_dependency_validation_snapshot_horizon();
  return commit;
}

void TransactionManager::_refresh_dependency_validation_snapshot_horizon() {
  const auto lock = std::lock_guard<std::mutex>{_active_snapshot_commit_ids_mutex};
  _refresh_dependency_validation_snapshot_horizon_locked();
}

void TransactionManager::_refresh_dependency_validation_snapshot_horizon_locked() {
  const auto lowest_snapshot = _active_snapshot_commit_ids.empty()
                                   ? std::optional<CommitID>{}
                                   : std::optional<CommitID>{std::ranges::min(_active_snapshot_commit_ids)};
  _dependency_validation_commit_coordinator->update_lowest_active_snapshot(lowest_snapshot);
}

void TransactionManager::_try_increment_last_commit_id(const std::shared_ptr<CommitContext>& context) {
  auto current_context = context;

  while (current_context->is_pending()) {
    auto expected_last_commit_id = CommitID{current_context->commit_id() - 1};

    // Metadata associated with this CID must be fully published before the
    // watermark makes its rows visible to newly created snapshots. Multiple
    // threads can help advance the chain, so CommitContext guarantees that the
    // callback executes exactly once and that all callers wait for it.
    if (_last_commit_id.load() != expected_last_commit_id) {
      return;
    }
    current_context->fire_prepublication_callback();
    dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::DvVisibleWatermarkNotAdvanced,
                                      current_context->commit_id());

    if (!_last_commit_id.compare_exchange_strong(expected_last_commit_id, current_context->commit_id())) {
      return;
    }

    current_context->fire_callback();

    if (!current_context->has_next()) {
      return;
    }

    current_context = current_context->next();
  }
}

}  // namespace hyrise
