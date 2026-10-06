#include "transaction_context.hpp"

#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <ostream>

#include "commit_context.hpp"  // IWYU pragma: keep
#include "hyrise.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "storage/dependency_validation/dependency_validation_commit_coordinator.hpp"
#include "storage/dependency_validation/dependency_validation_test_hooks.hpp"
#include "storage/dependency_validation/dependency_validation_write_set.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID snapshot_commit_id,
                                       const AutoCommit is_auto_commit)
    : TransactionContext{transaction_id, snapshot_commit_id, is_auto_commit, false} {}

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID snapshot_commit_id,
                                       const AutoCommit is_auto_commit, const bool snapshot_already_registered)
    : _transaction_id{transaction_id},
      _snapshot_commit_id{snapshot_commit_id},
      _is_auto_commit{is_auto_commit},
      _phase{TransactionPhase::Active},
      _num_active_operators{0} {
  if (!snapshot_already_registered) {
    Hyrise::get().transaction_manager._register_transaction(snapshot_commit_id);
  }
}

TransactionContext::~TransactionContext() {
  if constexpr (HYRISE_DEBUG) {
    // Note: When thrown during stack unwinding, exceptions from the following assertions might hide previous
    // exceptions. If you are seeing this, either use a debugger and break on exceptions or turn off these assertions as
    // a trial.
    const auto is_rolled_back_after_conflict = _phase == TransactionPhase::RolledBackAfterConflict;
    for (const auto& op : _read_write_operators) {
      const auto operator_has_conflict = op->state() == ReadWriteOperatorState::Conflicted;
      Assert(!operator_has_conflict || is_rolled_back_after_conflict,
             "A registered operator failed but the transaction has not been rolled back. You may also see this "
             "exception if an operator threw an uncaught exception.");
    }

    const auto has_registered_operators = !_read_write_operators.empty();
    const auto committed_or_rolled_back = _phase == TransactionPhase::Committed ||
                                          _phase == TransactionPhase::RolledBackByUser ||
                                          _phase == TransactionPhase::RolledBackAfterConflict;
    Assert(!has_registered_operators || committed_or_rolled_back,
           "Has registered operators but has neither been committed nor rolled back (see comment in code).");
  }

  // Tell the TransactionManager, which keeps track of active snapshot-commit-ids, that this transaction has finished.
  Hyrise::get().transaction_manager._deregister_transaction(_snapshot_commit_id);
}

TransactionID TransactionContext::transaction_id() const {
  return _transaction_id;
}

CommitID TransactionContext::snapshot_commit_id() const {
  return _snapshot_commit_id;
}

AutoCommit TransactionContext::is_auto_commit() const {
  return _is_auto_commit;
}

CommitID TransactionContext::commit_id() const {
  Assert(_commit_context, "TransactionContext CommitID only available after commit context has been created.");

  return _commit_context->commit_id();
}

TransactionPhase TransactionContext::phase() const {
  return _phase;
}

bool TransactionContext::aborted() const {
  const auto phase = _phase.load();
  return (phase == TransactionPhase::Conflicted) || (phase == TransactionPhase::RolledBackAfterConflict);
}

void TransactionContext::rollback(RollbackReason rollback_reason) {
  if (rollback_reason == RollbackReason::Conflict) {
    _mark_as_conflicted();
  } else {
    // We directly go to RolledBackByUser, skipping Conflicted
    Assert(_num_active_operators == 0, "For a user-initiated rollback, no operators should be active.");
  }

  for (const auto& op : _read_write_operators) {
    op->rollback_records();
  }

  // Rollback happens before a DV CID/ticket exists in Phase 6. Discarding the
  // transaction-local write set therefore cannot leave an index reservation.
  _dependency_validation_write_set.reset();

  _mark_as_rolled_back(rollback_reason);
}

void TransactionContext::commit_async(const std::function<void(TransactionID)>& callback) {
  _prepare_commit();

  dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::EffectsInstalledRowsNotCommitted,
                                    _commit_context->commit_id());

  for (const auto& op : _read_write_operators) {
    op->commit_records(commit_id());
  }

  dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::RowsCommittedContextNotPending,
                                    _commit_context->commit_id());

  _mark_as_pending_and_try_commit(callback);
}

void TransactionContext::stage_dependency_insert(std::shared_ptr<const Table> table,
                                                 std::shared_ptr<dv_tree::DVTree> tree, std::string dependency_name,
                                                 std::string lhs_norm, std::string rhs_norm) {
  Assert(_phase == TransactionPhase::Active, "Dependency changes can only be staged by an active transaction.");
  if (!_dependency_validation_write_set) {
    _dependency_validation_write_set = std::make_unique<dv_tree::DependencyValidationWriteSet>();
  }
  _dependency_validation_write_set->insert(std::move(table), std::move(tree), std::move(dependency_name),
                                           std::move(lhs_norm), std::move(rhs_norm));
  if (_dependency_validation_write_set->empty()) {
    _dependency_validation_write_set.reset();
  }
}

void TransactionContext::stage_dependency_remove(std::shared_ptr<const Table> table,
                                                 std::shared_ptr<dv_tree::DVTree> tree, std::string dependency_name,
                                                 std::string lhs_norm, std::string rhs_norm) {
  Assert(_phase == TransactionPhase::Active, "Dependency changes can only be staged by an active transaction.");
  if (!_dependency_validation_write_set) {
    _dependency_validation_write_set = std::make_unique<dv_tree::DependencyValidationWriteSet>();
  }
  _dependency_validation_write_set->remove(std::move(table), std::move(tree), std::move(dependency_name),
                                           std::move(lhs_norm), std::move(rhs_norm));
  if (_dependency_validation_write_set->empty()) {
    _dependency_validation_write_set.reset();
  }
}

void TransactionContext::stage_dependency_update(std::shared_ptr<const Table> table,
                                                 std::shared_ptr<dv_tree::DVTree> tree, std::string dependency_name,
                                                 std::string lhs_norm, std::string old_rhs_norm,
                                                 std::string new_rhs_norm) {
  Assert(_phase == TransactionPhase::Active, "Dependency changes can only be staged by an active transaction.");
  if (!_dependency_validation_write_set) {
    _dependency_validation_write_set = std::make_unique<dv_tree::DependencyValidationWriteSet>();
  }
  _dependency_validation_write_set->update(std::move(table), std::move(tree), std::move(dependency_name),
                                           std::move(lhs_norm), std::move(old_rhs_norm), std::move(new_rhs_norm));
  if (_dependency_validation_write_set->empty()) {
    _dependency_validation_write_set.reset();
  }
}

const dv_tree::DependencyValidationWriteSet* TransactionContext::dependency_validation_write_set() const {
  return _dependency_validation_write_set.get();
}

void TransactionContext::commit() {
  Assert(_phase == TransactionPhase::Active, "TransactionContext must be active to be committed.");

  // No modifications made, nothing to commit, no need to acquire a commit ID
  if (_read_write_operators.empty()) {
    _transition(TransactionPhase::Active, TransactionPhase::Committed);
    return;
  }

  auto committed = std::promise<void>{};
  const auto committed_future = committed.get_future();
  const auto callback = [&committed](TransactionID /*unused*/) {
    committed.set_value();
  };

  commit_async(callback);

  committed_future.wait();
}

void TransactionContext::_mark_as_conflicted() {
  _transition(TransactionPhase::Active, TransactionPhase::Conflicted);

  _wait_for_active_operators_to_finish();
}

void TransactionContext::_mark_as_rolled_back(RollbackReason rollback_reason) {
  if constexpr (HYRISE_DEBUG) {
    for (const auto& op : _read_write_operators) {
      Assert(op->state() == ReadWriteOperatorState::RolledBack, "All read/write operators must have been rolled back.");
    }
  }

  if (rollback_reason == RollbackReason::User) {
    _transition(TransactionPhase::Active, TransactionPhase::RolledBackByUser);
  } else {
    DebugAssert(rollback_reason == RollbackReason::Conflict, "Invalid RollbackReason.");
    _transition(TransactionPhase::Conflicted, TransactionPhase::RolledBackAfterConflict);
  }
}

void TransactionContext::_prepare_commit() {
  if constexpr (HYRISE_DEBUG) {
    for (const auto& op : _read_write_operators) {
      Assert(op->state() == ReadWriteOperatorState::Executed,
             "All read/write operators must have been executed (especially not failed).");
    }
  }

  _transition(TransactionPhase::Active, TransactionPhase::Committing);

  _wait_for_active_operators_to_finish();

  _commit_context = Hyrise::get().transaction_manager._new_commit_context();
  dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::CidAssignedFootprintNotRegistered,
                                    _commit_context->commit_id());
  try {
    _dependency_validation_commit = Hyrise::get().transaction_manager._register_dependency_validation_commit(
        _commit_context->commit_id(), _dependency_validation_write_set.get());

    dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::FootprintRegisteredPreparationNotStarted,
                                      _commit_context->commit_id());

    // All tickets are registered before any one is awaited. The waits happen
    // outside the coordinator and complete the private DV installation before
    // row MVCC CIDs are written by commit_records().
    if (_dependency_validation_commit) {
      _dependency_validation_commit->seal_and_wait_until_applied();
    }
  } catch (...) {
    // A ticket that has already begun installation cannot be rolled back
    // safely. Publishing the row changes without matching DV effects would be
    // corrupt, so fail loudly instead of continuing in-process.
    if (_dependency_validation_commit && !_dependency_validation_commit->abort_before_row_commit()) {
      std::terminate();
    }
    _retire_failed_commit_after_cid();
    throw;
  }
}

void TransactionContext::_mark_as_pending_and_try_commit(const std::function<void(TransactionID)>& callback) {
  if constexpr (HYRISE_DEBUG) {
    for (const auto& op : _read_write_operators) {
      const auto expected_state =
          _commit_failed_after_cid ? ReadWriteOperatorState::RolledBack : ReadWriteOperatorState::Committed;
      Assert(op->state() == expected_state, "Unexpected read-write operator state while publishing a commit context.");
    }
  }

  auto context_weak_ptr = std::weak_ptr<TransactionContext>{this->shared_from_this()};
  // CommitContext owns this state until ordered publication. DV visibility
  // must not depend on the initiating TransactionContext remaining alive.
  auto dependency_validation_commit =
      std::shared_ptr<dv_tree::DependencyValidationCommit>{std::move(_dependency_validation_commit)};
  const auto publish_dependency_visibility = [dependency_validation_commit] {
    if (dependency_validation_commit) {
      dependency_validation_commit->publish_visibility();
    }
  };
  _commit_context->make_pending(
      _transaction_id,
      [context_weak_ptr, callback](auto transaction_id) {
        // If the transaction context still exists, set its phase to Committed.
        if (auto context_ptr = context_weak_ptr.lock()) {
          context_ptr->_transition(TransactionPhase::Committing, context_ptr->_commit_failed_after_cid
                                                                     ? TransactionPhase::RolledBackAfterConflict
                                                                     : TransactionPhase::Committed);
        }

        if (callback) {
          callback(transaction_id);
        }
      },
      publish_dependency_visibility);

  Hyrise::get().transaction_manager._try_increment_last_commit_id(_commit_context);

  // Pending but not yet published means a lower CID has not published; neither
  // this commit's rows nor its DV metadata may be visible yet.
  if (Hyrise::get().transaction_manager.last_commit_id() < _commit_context->commit_id()) {
    dv_tree::CommitPauseHooks::notify(dv_tree::CommitPausePoint::ContextPendingBehindLowerCid,
                                      _commit_context->commit_id());
  }
}

void TransactionContext::_retire_failed_commit_after_cid() {
  _commit_failed_after_cid = true;
  _dependency_validation_commit.reset();
  _dependency_validation_write_set.reset();

  for (const auto& op : _read_write_operators) {
    op->rollback_records();
  }

  // No operator commit_records() has run. Making this CID pending retires it
  // from Hyrise's ordered chain, and the callback changes the context into the
  // conflict-style rollback state once all lower CIDs have been published.
  _mark_as_pending_and_try_commit({});
}

void TransactionContext::on_operator_started() {
  ++_num_active_operators;
}

void TransactionContext::on_operator_finished() {
  DebugAssert(_num_active_operators > 0, "Unexpected number of active operators.");
  const auto num_before = _num_active_operators--;

  if (num_before == 1) {
    _active_operators_cv.notify_all();
  }
}

bool TransactionContext::is_auto_commit() {
  return _is_auto_commit == AutoCommit::Yes;
}

void TransactionContext::_wait_for_active_operators_to_finish() const {
  std::unique_lock<std::mutex> lock(_active_operators_mutex);
  if (_num_active_operators == 0) {
    return;
  }
  _active_operators_cv.wait(lock, [&] {
    return _num_active_operators != 0;
  });
}

void TransactionContext::_transition(TransactionPhase from_phase, TransactionPhase to_phase) {
  DebugAssert(_is_auto_commit == AutoCommit::No || to_phase != TransactionPhase::RolledBackByUser,
              "Auto-commit transactions cannot be manually rolled back.");
  const auto success = _phase.compare_exchange_strong(from_phase, to_phase);
  Assert(success, "Illegal phase transition.");
}

std::ostream& operator<<(std::ostream& stream, const TransactionPhase& phase) {
  switch (phase) {
    case TransactionPhase::Active:
      stream << "Active";
      break;
    case TransactionPhase::Conflicted:
      stream << "Conflicted";
      break;
    case TransactionPhase::RolledBackAfterConflict:
      stream << "RolledBackAfterConflict";
      break;
    case TransactionPhase::RolledBackByUser:
      stream << "RolledBackByUser";
      break;
    case TransactionPhase::Committing:
      stream << "Committing";
      break;
    case TransactionPhase::Committed:
      stream << "Committed";
      break;
  }
  return stream;
}

}  // namespace hyrise
