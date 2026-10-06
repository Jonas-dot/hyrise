#include "storage/dependency_validation/dependency_validation_write_set.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <utility>

#include "utils/assert.hpp"

namespace hyrise::dv_tree {

namespace {

struct OperationKey {
  std::string lhs_norm;
  std::string rhs_norm;
  int64_t delta = 0;
};

// Phase 6 uses a compact logical operation list. We rebuild the public core
// Transaction after each staging call: duplicate inserts retain multiplicity,
// while inverse operations on the same normalized tuple cancel before a CID is
// assigned. The write sets are expected to be small; Phase 15 can replace this
// linear representation if profiling demonstrates a need.
void apply_delta(Transaction& transaction, std::string lhs_norm, std::string rhs_norm, const int64_t delta) {
  auto operations = std::vector<OperationKey>{};
  operations.reserve(transaction.ops.size() + 1);

  for (const auto& operation : transaction.ops) {
    const auto existing = std::ranges::find_if(operations, [&](const auto& candidate) {
      return candidate.lhs_norm == operation.lhs_norm && candidate.rhs_norm == operation.rhs_norm;
    });
    if (existing == operations.cend()) {
      operations.push_back(
          {.lhs_norm = operation.lhs_norm, .rhs_norm = operation.rhs_norm, .delta = operation.is_delete ? -1 : 1});
    } else {
      existing->delta += operation.is_delete ? -1 : 1;
    }
  }

  const auto target = std::ranges::find_if(operations, [&](const auto& candidate) {
    return candidate.lhs_norm == lhs_norm && candidate.rhs_norm == rhs_norm;
  });
  if (target == operations.cend()) {
    operations.push_back({.lhs_norm = std::move(lhs_norm), .rhs_norm = std::move(rhs_norm), .delta = delta});
  } else {
    Assert((delta > 0 && target->delta < std::numeric_limits<int64_t>::max()) ||
               (delta < 0 && target->delta > std::numeric_limits<int64_t>::min()),
           "Dependency-validation operation multiplicity overflow.");
    target->delta += delta;
  }

  transaction.ops.clear();
  for (auto& operation : operations) {
    if (operation.delta > 0) {
      for (auto count = int64_t{0}; count < operation.delta; ++count) {
        transaction.insert(operation.lhs_norm, operation.rhs_norm);
      }
    } else {
      for (auto count = int64_t{0}; count > operation.delta; --count) {
        transaction.remove(operation.lhs_norm, operation.rhs_norm);
      }
    }
  }
}

}  // namespace

void DependencyValidationWriteSet::insert(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree,
                                          std::string dependency_name, std::string lhs_norm, std::string rhs_norm) {
  _stage(_batch_for(std::move(table), std::move(tree), std::move(dependency_name)), std::move(lhs_norm),
         std::move(rhs_norm), 1);
  _discard_empty_batches();
}

void DependencyValidationWriteSet::remove(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree,
                                          std::string dependency_name, std::string lhs_norm, std::string rhs_norm) {
  _stage(_batch_for(std::move(table), std::move(tree), std::move(dependency_name)), std::move(lhs_norm),
         std::move(rhs_norm), -1);
  _discard_empty_batches();
}

void DependencyValidationWriteSet::update(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree,
                                          std::string dependency_name, std::string lhs_norm, std::string old_rhs_norm,
                                          std::string new_rhs_norm) {
  auto& batch = _batch_for(std::move(table), std::move(tree), std::move(dependency_name));
  _stage(batch, lhs_norm, std::move(old_rhs_norm), -1);
  _stage(batch, std::move(lhs_norm), std::move(new_rhs_norm), 1);
  _discard_empty_batches();
}

bool DependencyValidationWriteSet::empty() const {
  return _batches.empty();
}

const std::vector<DependencyValidationWriteSet::Batch>& DependencyValidationWriteSet::batches() const {
  return _batches;
}

void DependencyValidationWriteSet::clear() {
  _batches.clear();
}

DependencyValidationWriteSet::Batch& DependencyValidationWriteSet::_batch_for(std::shared_ptr<const Table> table,
                                                                              std::shared_ptr<DVTree> tree,
                                                                              std::string dependency_name) {
  Assert(table, "Dependency-validation write batch needs a table.");
  Assert(tree, "Dependency-validation write batch needs a DVTree.");

  const auto existing = std::ranges::find_if(_batches, [&](const auto& batch) {
    return batch.tree == tree;
  });
  if (existing != _batches.cend()) {
    // GetTable creates a lightweight data-table wrapper around stored chunks.
    // Delete stages old rows through that wrapper while Insert stages new rows
    // through the stored table. Both descriptors deliberately share the same
    // DVTree, which is the actual dependency identity. Retain the first table
    // reference for lifetime safety but do not reject equivalent wrappers.
    Assert(existing->dependency_name == dependency_name, "Inconsistent dependency metadata for one DVTree.");
    return *existing;
  }

  _batches.push_back({.table = std::move(table),
                      .tree = std::move(tree),
                      .dependency_name = std::move(dependency_name),
                      .transaction = {}});
  return _batches.back();
}

void DependencyValidationWriteSet::_stage(Batch& batch, std::string lhs_norm, std::string rhs_norm,
                                          const int64_t delta) {
  Assert(!lhs_norm.empty(), "Normalized dependency LHS must not be empty.");
  Assert(!rhs_norm.empty(), "Normalized dependency RHS must not be empty.");
  apply_delta(batch.transaction, std::move(lhs_norm), std::move(rhs_norm), delta);
}

void DependencyValidationWriteSet::_discard_empty_batches() {
  std::erase_if(_batches, [](const auto& batch) {
    return batch.transaction.ops.empty();
  });
}

}  // namespace hyrise::dv_tree
