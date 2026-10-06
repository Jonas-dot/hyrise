#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/dependency_validation/dv_tree.hpp"
#include "types.hpp"

namespace hyrise {
class Table;
}  // namespace hyrise

namespace hyrise::dv_tree {

// Immutable description of the columns one validator covers, plus its kind.
// Phase 12 bootstrap uses it to read and normalize the right columns.
struct DependencySpec {
  DependencyKind kind;
  std::vector<ColumnID> lhs_column_ids;
  std::vector<ColumnID> rhs_column_ids;
  std::vector<DataType> lhs_column_types;
  std::vector<DataType> rhs_column_types;
};

// One dependency row after normalization: the byte-encoded LHS and RHS keys.
using NormalizedDependencyRow = std::pair<std::string, std::string>;

// Scans exactly the rows of `table` visible at MVCC snapshot `build_cid` and
// returns their normalized (LHS, RHS) keys.
//
// Only valid on a quiescent table (no in-flight writers). For such a table a
// committed row belongs to the snapshot iff begin_cid <= build_cid < end_cid --
// this is Validate::is_row_visible reduced to the case of a committed row with
// no active writer. Rows inserted after `build_cid` and rows already deleted at
// or before it are excluded, so the returned set is drawn from the single
// snapshot `build_cid` and construction never mixes snapshots.
std::vector<NormalizedDependencyRow> scan_visible_dependency_rows(const Table& table, const DependencySpec& spec,
                                                                  CommitID build_cid);

// Independent batch oracle for FUNCTIONAL dependencies: the violation count
// computed directly from the raw rows as the sum over LHS groups of
// (distinct RHS values - 1). It shares no state with the DVTree engine, so it
// can cross-check a freshly built tree.
int64_t batch_fd_violation_count(const std::vector<NormalizedDependencyRow>& rows);

// Independent batch oracle for ORDER dependencies. In addition to the FD-like
// local count within each LHS group, it counts one violation for every adjacent
// non-empty LHS group whose maximum RHS sorts after its successor's minimum RHS.
// All comparisons are made directly over the normalized key bytes.
int64_t batch_od_violation_count(const std::vector<NormalizedDependencyRow>& rows);

// Builds a private DVTree from exactly the rows visible at `build_cid`.
//
// The tree's visible snapshot and history baseline are set to `build_cid`, so
// the whole pre-existing dataset appears as one atomic state at that CID; later
// Hyrise commits (CID > build_cid) extend it through the commit coordinator. For
// the result is asserted equal to the corresponding independent FD or OD batch
// oracle, catching any build error at construction time.
std::shared_ptr<DVTree> build_dependency_validator(const Table& table, const DependencySpec& spec, CommitID build_cid);

}  // namespace hyrise::dv_tree
