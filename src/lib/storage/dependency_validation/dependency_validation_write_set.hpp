#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "storage/dependency_validation/dv_tree.hpp"

namespace hyrise {

class Table;

namespace dv_tree {

// Transaction-local, pre-CID collection of normalized dependency changes.
// It deliberately owns shared references to both the table and DVTree, so a
// table replacement/drop cannot invalidate a write that is still executing.
// Phase 8 consumes these batches to create external-CID DVTree tickets.
class DependencyValidationWriteSet {
 public:
  struct Batch {
    std::shared_ptr<const Table> table;
    std::shared_ptr<DVTree> tree;
    std::string dependency_name;
    Transaction transaction;
  };

  void insert(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree, std::string dependency_name,
              std::string lhs_norm, std::string rhs_norm);
  void remove(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree, std::string dependency_name,
              std::string lhs_norm, std::string rhs_norm);
  void update(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree, std::string dependency_name,
              std::string lhs_norm, std::string old_rhs_norm, std::string new_rhs_norm);

  bool empty() const;
  const std::vector<Batch>& batches() const;
  void clear();

 private:
  Batch& _batch_for(std::shared_ptr<const Table> table, std::shared_ptr<DVTree> tree, std::string dependency_name);
  void _stage(Batch& batch, std::string lhs_norm, std::string rhs_norm, int64_t delta);
  void _discard_empty_batches();

  std::vector<Batch> _batches;
};

}  // namespace dv_tree
}  // namespace hyrise
