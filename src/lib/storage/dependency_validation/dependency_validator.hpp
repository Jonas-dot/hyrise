#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "storage/dependency_validation/dv_tree.hpp"
#include "types.hpp"

namespace hyrise::dv_tree {

// Phase 11 public validation API.
//
// A read-only, MVCC-snapshot-safe view of one dependency validator registered
// on a Table. It exposes the dependency's descriptor (name, kind, LHS/RHS
// columns) and forwards only the snapshot-clamped verdict queries of the
// underlying DVTree.
//
// The tree is held as std::shared_ptr<const DVTree>, so neither a consumer nor
// this class can reach the mutating tree operations (begin_commit(),
// apply_commit(), the frontier advances, or set_lowest_active_snapshot()) --
// those are non-const and therefore uncallable through this handle. Every
// verdict query requires an explicit snapshot CommitID: the caller passes its
// transaction's snapshot_commit_id(). The tree throws (SnapshotNotVisible for a
// too-new snapshot, SnapshotHorizonViolation for one older than the retained
// history horizon) instead of silently answering outside the valid window.
// Together these make it impossible to accidentally bypass snapshot isolation,
// which is Phase 11's exit criterion.
//
// Mutable tree access is deliberately confined to the internal staging,
// bootstrap, and commit-coordinator boundary. Registration, mutation, and
// snapshot queries therefore remain separate surfaces.
class DependencyValidator {
 public:
  DependencyValidator(std::string name, DependencyKind kind, std::vector<ColumnID> lhs_columns,
                      std::vector<ColumnID> rhs_columns, std::shared_ptr<const DVTree> tree);

  const std::string& name() const;
  DependencyKind kind() const;
  const std::vector<ColumnID>& lhs_columns() const;
  const std::vector<ColumnID>& rhs_columns() const;

  // Does the dependency hold at the given MVCC snapshot? Pass your
  // transaction's snapshot_commit_id(). Any globally published snapshot is
  // answerable, even when newer than this tree's own frontier (commits that do
  // not touch a dependency never change its verdict). Throws if the snapshot
  // is beyond the global commit watermark or older than the retained history
  // horizon.
  bool holds_at(CommitID snapshot) const;

  // Number of violations at the given MVCC snapshot; 0 exactly when holds_at()
  // is true. Same snapshot contract and exceptions as holds_at().
  int64_t violation_count_at(CommitID snapshot) const;

  // Newest globally visible CID that may be queried.
  CommitID visible_commit_id() const;

  // Oldest snapshot whose exact verdict is still retained. Snapshots older than
  // this have been folded away and can no longer be queried exactly.
  CommitID oldest_exact_snapshot() const;

  DVTree::MemoryStatistics memory_statistics() const;

 private:
  // Rejects snapshots beyond Hyrise's global watermark, then maps a published
  // snapshot to the exact tree snapshot or to the tree's latest change point
  // when intervening commits did not touch this dependency.
  CommitID _effective_snapshot(CommitID snapshot) const;

  std::string _name;
  DependencyKind _kind;
  std::vector<ColumnID> _lhs_columns;
  std::vector<ColumnID> _rhs_columns;
  std::shared_ptr<const DVTree> _tree;
};

}  // namespace hyrise::dv_tree
