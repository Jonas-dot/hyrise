#include "storage/dependency_validation/dependency_validation_commit_coordinator.hpp"

#include <algorithm>
#include <utility>

#include "storage/dependency_validation/dv_tree_access.hpp"
#include "utils/assert.hpp"

namespace hyrise::dv_tree {

DependencyValidationCommit::DependencyValidationCommit(const CommitID commit_id, std::vector<TicketBinding> bindings)
    : _commit_id{commit_id}, _bindings{std::move(bindings)} {}

void DependencyValidationCommit::seal_and_wait_until_applied() {
  for (auto& binding : _bindings) {
    binding.ticket.seal();
  }
  for (const auto& binding : _bindings) {
    binding.ticket.wait_until_applied();
  }
}

bool DependencyValidationCommit::abort_before_row_commit() noexcept {
  try {
    for (auto& binding : _bindings) {
      binding.ticket.abort();
    }
    _bindings.clear();
    return true;
  } catch (...) {
    return false;
  }
}

void DependencyValidationCommit::publish_visibility() {
  Assert(!_published, "Dependency-validation visibility may only be published once per transaction.");
  for (const auto& binding : _bindings) {
    DVTreeAccess::advance_visibility_frontier(*binding.tree, _commit_id);
  }
  _published = true;
}

bool DependencyValidationCommit::empty() const {
  return _bindings.empty();
}

DependencyValidationCommitCoordinator::DependencyValidationCommitCoordinator(
    std::function<void()> before_ticket_creation)
    : _next_commit_id_to_register{static_cast<CommitID::base_type>(INITIAL_COMMIT_ID) + 1},
      _next_commit_id_to_create{static_cast<CommitID::base_type>(INITIAL_COMMIT_ID) + 1},
      _before_ticket_creation{std::move(before_ticket_creation)} {}

void DependencyValidationCommitCoordinator::reserve_commit_id(const CommitID commit_id) {
  const auto raw_commit_id = static_cast<CommitID::base_type>(commit_id);
  const auto guard = std::lock_guard<std::mutex>{_mutex};
  Assert(raw_commit_id >= _next_commit_id_to_register,
         "DV commit IDs must be reserved in Hyrise CID order or before their registration frontier.");
  const auto [_, inserted] = _pending_commits.emplace(raw_commit_id, PendingCommit{});
  Assert(inserted, "A Hyrise commit ID was reserved for dependency validation twice.");
}

std::unique_ptr<DependencyValidationCommit> DependencyValidationCommitCoordinator::register_commit(
    const CommitID commit_id, const DependencyValidationWriteSet* write_set) {
  const auto raw_commit_id = static_cast<CommitID::base_type>(commit_id);
  auto batches = std::vector<DependencyValidationWriteSet::Batch>{};
  auto registration_error = std::exception_ptr{};
  try {
    if (write_set != nullptr) {
      batches = write_set->batches();
    }
  } catch (...) {
    // The CID is already reserved. Register an empty retired entry below so
    // the coordinator frontier and all later CIDs can still progress, then
    // rethrow after this entry has been drained.
    registration_error = std::current_exception();
  }

  {
    const auto guard = std::lock_guard<std::mutex>{_mutex};
    const auto found = _pending_commits.find(raw_commit_id);
    Assert(found != _pending_commits.end(), "Dependency-validation registration requires a reserved Hyrise commit ID.");
    Assert(!found->second.registered, "Dependency-validation write set was registered twice for one commit ID.");

    found->second.registered = true;
    found->second.batches = std::move(batches);
    found->second.error = registration_error;

    while (true) {
      const auto next = _pending_commits.find(_next_commit_id_to_register);
      if (next == _pending_commits.end() || !next->second.registered) {
        break;
      }
      ++_next_commit_id_to_register;
    }
  }
  _cv.notify_all();

  _create_ready_tickets();

  auto guard = std::unique_lock<std::mutex>{_mutex};
  _cv.wait(guard, [&] {
    const auto found = _pending_commits.find(raw_commit_id);
    return found != _pending_commits.end() && found->second.tickets_created;
  });

  auto found = _pending_commits.find(raw_commit_id);
  auto bindings = std::move(found->second.bindings);
  const auto error = found->second.error;
  _pending_commits.erase(found);

  if (error) {
    std::rethrow_exception(error);
  }

  if (bindings.empty()) {
    return nullptr;
  }
  return std::unique_ptr<DependencyValidationCommit>{new DependencyValidationCommit{commit_id, std::move(bindings)}};
}

void DependencyValidationCommitCoordinator::_create_ready_tickets() {
  const auto creation_guard = std::lock_guard<std::mutex>{_ticket_creation_mutex};

  while (true) {
    CommitID commit_id = UNSET_COMMIT_ID;
    std::vector<DependencyValidationWriteSet::Batch> batches;
    std::exception_ptr existing_error;
    {
      const auto guard = std::lock_guard<std::mutex>{_mutex};
      const auto found = _pending_commits.find(_next_commit_id_to_create);
      if (found == _pending_commits.end() || !found->second.registered || found->second.tickets_created) {
        return;
      }
      commit_id = CommitID{_next_commit_id_to_create};
      batches = std::move(found->second.batches);
      existing_error = found->second.error;
    }

    std::vector<DependencyValidationCommit::TicketBinding> bindings;
    auto creation_error = existing_error;
    if (!creation_error) {
      try {
        bindings.reserve(batches.size());
        for (const auto& batch : batches) {
          if (_before_ticket_creation) {
            _before_ticket_creation();
          }
          if (commit_id > CommitID{static_cast<CommitID::base_type>(INITIAL_COMMIT_ID) + 1}) {
            DVTreeAccess::advance_registration_frontier(*batch.tree,
                                                        CommitID{static_cast<CommitID::base_type>(commit_id) - 1});
          }
          auto ticket = DVTreeAccess::begin_commit(*batch.tree, commit_id);
          bindings.push_back({.tree = batch.tree, .ticket = std::move(ticket)});
          // Stage this batch's collected inserts/removes onto the ticket.
          // Construction is still private and abortable at this point.
          bindings.back().ticket.stage(batch.transaction);
        }
      } catch (...) {
        creation_error = std::current_exception();
      }
    }

    if (creation_error) {
      // Every successfully constructed ticket is still private. Retire it and
      // close its tree-local registration frontier before allowing a later CID
      // to proceed. Failure here would make safe in-process recovery
      // impossible, so preserve the existing fail-stop policy.
      for (auto& binding : bindings) {
        try {
          binding.ticket.abort();
          DVTreeAccess::advance_registration_frontier(*binding.tree, commit_id);
          _remember_tree(binding.tree);
        } catch (...) {
          std::terminate();
        }
      }
      bindings.clear();
    }

    for (const auto& binding : bindings) {
      // The current CID has now registered every one of its tickets for this
      // tree, so later CIDs may safely start scheduling against it.
      DVTreeAccess::advance_registration_frontier(*binding.tree, commit_id);
      _remember_tree(binding.tree);
    }

    {
      const auto guard = std::lock_guard<std::mutex>{_mutex};
      const auto found = _pending_commits.find(_next_commit_id_to_create);
      Assert(found != _pending_commits.end() && found->second.registered,
             "A ready dependency-validation registration disappeared during ticket creation.");
      found->second.bindings = std::move(bindings);
      found->second.error = creation_error;
      found->second.tickets_created = true;
      ++_next_commit_id_to_create;
    }
    _cv.notify_all();
  }
}

void DependencyValidationCommitCoordinator::update_lowest_active_snapshot(
    const std::optional<CommitID> lowest_snapshot) {
  std::vector<std::shared_ptr<DVTree>> trees;
  {
    const auto guard = std::lock_guard<std::mutex>{_mutex};
    std::erase_if(_known_trees, [](const auto& weak_tree) {
      return weak_tree.expired();
    });
    trees.reserve(_known_trees.size());
    for (const auto& weak_tree : _known_trees) {
      if (const auto tree = weak_tree.lock()) {
        trees.push_back(tree);
      }
    }
  }

  for (const auto& tree : trees) {
    if (lowest_snapshot) {
      DVTreeAccess::set_lowest_active_snapshot(*tree, *lowest_snapshot);
    } else {
      DVTreeAccess::clear_lowest_active_snapshot(*tree);
    }
  }
}

void DependencyValidationCommitCoordinator::_remember_tree(const std::shared_ptr<DVTree>& tree) {
  const auto guard = std::lock_guard<std::mutex>{_mutex};
  const auto already_known = std::ranges::any_of(_known_trees, [&](const auto& weak_tree) {
    return weak_tree.lock() == tree;
  });
  if (!already_known) {
    _known_trees.emplace_back(tree);
  }
}

}  // namespace hyrise::dv_tree
