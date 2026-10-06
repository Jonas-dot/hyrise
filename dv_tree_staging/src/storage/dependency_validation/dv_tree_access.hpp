#pragma once

#include <memory>

#include "storage/dependency_validation/dv_tree.hpp"

namespace hyrise::dv_tree {

// Private integration boundary for the bootstrap, commit coordinator, and
// snapshot-retention manager. Ordinary consumers must use DependencyValidator,
// which checks Hyrise's global visibility watermark before querying a snapshot.
class DVTreeAccess {
 public:
  static DVTree::CommitTicket begin_commit(DVTree& tree, CommitID commit_id) {
    return tree.begin_commit(commit_id);
  }

  static CommitID apply_commit(DVTree& tree, const Transaction& transaction) {
    return tree.apply_commit(transaction);
  }

  static void advance_registration_frontier(DVTree& tree, CommitID commit_id) {
    tree.advance_registration_frontier(commit_id);
  }

  static void advance_visibility_frontier(DVTree& tree, CommitID commit_id) {
    tree.advance_visibility_frontier(commit_id);
  }

  static void set_lowest_active_snapshot(DVTree& tree, CommitID commit_id) {
    tree.set_lowest_active_snapshot(commit_id);
  }

  static void clear_lowest_active_snapshot(DVTree& tree) {
    tree.clear_lowest_active_snapshot();
  }

  static bool holds_at(const DVTree& tree, CommitID snapshot) {
    return tree.holds_at(snapshot);
  }

  static int64_t violation_count_at(const DVTree& tree, CommitID snapshot) {
    return tree.violation_count_at(snapshot);
  }

  static CommitID visible_commit_id(const DVTree& tree) {
    return tree.visible_commit_id();
  }
};

}  // namespace hyrise::dv_tree
