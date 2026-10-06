#pragma once

#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "storage/dependency_validation/dependency_validation_write_set.hpp"
#include "types.hpp"

namespace hyrise::dv_tree {

// Owns the sealed external-CID tickets of one Hyrise transaction. The tickets
// are installed before Hyrise writes row CIDs; publication remains exclusively
// controlled by Hyrise's CommitContext chain.
class DependencyValidationCommit {
 public:
  DependencyValidationCommit() = default;
  DependencyValidationCommit(const DependencyValidationCommit&) = delete;
  DependencyValidationCommit& operator=(const DependencyValidationCommit&) = delete;
  DependencyValidationCommit(DependencyValidationCommit&&) noexcept = default;
  DependencyValidationCommit& operator=(DependencyValidationCommit&&) noexcept = default;

  void seal_and_wait_until_applied();
  // Retires every still-uninstalled ticket. This is exclusively the emergency
  // path between CID allocation and row-CID publication. A ticket whose
  // effects have started installing cannot be safely undone and returns false.
  bool abort_before_row_commit() noexcept;
  void publish_visibility();
  bool empty() const;

 private:
  struct TicketBinding {
    std::shared_ptr<DVTree> tree;
    DVTree::CommitTicket ticket;
  };

  explicit DependencyValidationCommit(CommitID commit_id, std::vector<TicketBinding> bindings);

  CommitID _commit_id = UNSET_COMMIT_ID;
  std::vector<TicketBinding> _bindings;
  bool _published = false;

  friend class DependencyValidationCommitCoordinator;
};

// Coordinates only DV registration metadata. Every Hyrise-issued write CID is
// reserved immediately, then receives one complete (possibly empty) write-set
// registration. This lets a higher CID prove that no lower footprint can still
// arrive, without turning the DV layer into a second transaction publisher.
class DependencyValidationCommitCoordinator {
 public:
  explicit DependencyValidationCommitCoordinator(std::function<void()> before_ticket_creation = {});
  DependencyValidationCommitCoordinator(const DependencyValidationCommitCoordinator&) = delete;
  DependencyValidationCommitCoordinator& operator=(const DependencyValidationCommitCoordinator&) = delete;

  void reserve_commit_id(CommitID commit_id);
  std::unique_ptr<DependencyValidationCommit> register_commit(CommitID commit_id,
                                                              const DependencyValidationWriteSet* write_set);

  // Hyrise calls this whenever its active-transaction snapshot set changes.
  // Weak ownership avoids extending a dropped table/tree's lifetime.
  void update_lowest_active_snapshot(std::optional<CommitID> lowest_snapshot);

 private:
  struct PendingCommit {
    bool registered = false;
    bool tickets_created = false;
    std::exception_ptr error;
    std::vector<DependencyValidationWriteSet::Batch> batches;
    std::vector<DependencyValidationCommit::TicketBinding> bindings;
  };

  void _create_ready_tickets();
  void _remember_tree(const std::shared_ptr<DVTree>& tree);

  std::mutex _mutex;
  std::condition_variable _cv;
  std::map<CommitID::base_type, PendingCommit> _pending_commits;
  CommitID::base_type _next_commit_id_to_register;
  CommitID::base_type _next_commit_id_to_create;
  std::vector<std::weak_ptr<DVTree>> _known_trees;
  std::function<void()> _before_ticket_creation;

  // This serializes only CID-ordered ticket construction. It is never held
  // while a ticket is sealed, prepared, installed, or waited upon.
  std::mutex _ticket_creation_mutex;
};

}  // namespace hyrise::dv_tree
