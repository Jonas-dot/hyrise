#include "storage/dependency_validation/dependency_validator.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "storage/dependency_validation/dv_tree.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise::dv_tree {

DependencyValidator::DependencyValidator(std::string name, DependencyKind kind, std::vector<ColumnID> lhs_columns,
                                         std::vector<ColumnID> rhs_columns, std::shared_ptr<const DVTree> tree)
    : _name{std::move(name)},
      _kind{kind},
      _lhs_columns{std::move(lhs_columns)},
      _rhs_columns{std::move(rhs_columns)},
      _tree{std::move(tree)} {
  Assert(_tree, "A dependency validator handle needs a DVTree.");
}

const std::string& DependencyValidator::name() const {
  return _name;
}

DependencyKind DependencyValidator::kind() const {
  return _kind;
}

const std::vector<ColumnID>& DependencyValidator::lhs_columns() const {
  return _lhs_columns;
}

const std::vector<ColumnID>& DependencyValidator::rhs_columns() const {
  return _rhs_columns;
}

bool DependencyValidator::holds_at(const CommitID snapshot) const {
  return _tree->holds_at(_effective_snapshot(snapshot));
}

int64_t DependencyValidator::violation_count_at(const CommitID snapshot) const {
  return _tree->violation_count_at(_effective_snapshot(snapshot));
}

CommitID DependencyValidator::_effective_snapshot(const CommitID snapshot) const {
  const auto global_visible = Hyrise::get().transaction_manager.last_commit_id();
  if (snapshot > global_visible) {
    throw SnapshotNotVisible{"snapshot commit id is not globally visible yet"};
  }

  const auto visible = _tree->visible_commit_id();
  if (snapshot <= visible) {
    return snapshot;
  }
  // The tree's frontier only advances when a commit that touches this
  // dependency is published, so a globally published snapshot beyond the
  // frontier consists purely of commits that left this dependency unchanged --
  // its verdict is exactly the frontier's verdict. Snapshots beyond the global
  // tree's verdict is exactly the frontier's verdict.
  return visible;
}

CommitID DependencyValidator::visible_commit_id() const {
  return std::min(_tree->visible_commit_id(), Hyrise::get().transaction_manager.last_commit_id());
}

CommitID DependencyValidator::oldest_exact_snapshot() const {
  return _tree->memory_statistics().oldest_exact_snapshot;
}

DVTree::MemoryStatistics DependencyValidator::memory_statistics() const {
  return _tree->memory_statistics();
}

}  // namespace hyrise::dv_tree
