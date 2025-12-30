#pragma once

#include <memory>
#include <unordered_set>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

class BTreeNode;

/**
 * Dependency type for validation.
 * - FD: Functional Dependency (A → B): Same LHS must have same RHS
 * - OD: Order Dependency (A ~ B): LHS ordering implies RHS ordering
 */
enum class DependencyType { FD, OD };

/**
 * Result structure for validation operations.
 * Tracks how metadata changes should affect the global violation counter.
 */
struct MetadataDeltas {
  int flag_delta = 0;                   // Change in right_neighbor_flag violations
  int local_violation_count_delta = 0;  // Change in local (duplicate) violations

  int total_delta() const {
    return flag_delta + local_violation_count_delta;
  }
};

/**
 * Value stored in B-Tree entries with metadata for dependency validation.
 *
 * For FD validation:
 * - rhs_values: Set of distinct RHS values for this LHS key
 * - local_violation_count = distinct_rhs_count - 1 (same LHS, different RHS = violation)
 * - right_neighbor_flag: Not used (FD only cares about same-key relationships)
 *
 * For OD validation:
 * - rhs_values: Set of distinct RHS values for this LHS key (same LHS, different RHS = ambiguous ordering)
 * - min_rhs, max_rhs: Min/max RHS values for boundary violation detection
 * - local_violation_count = distinct_rhs_count - 1 (same LHS, different RHS = violation)
 * - right_neighbor_flag: 1 if max_rhs > min_rhs of right neighbor (order violation between keys)
 */
struct BTreeValue {
  ChunkOffset start_index;
  ChunkOffset count;

  // For FD and OD: tracks distinct RHS values
  std::unordered_set<AllTypeVariant> rhs_values;

  // For OD: tracks min/max RHS values for boundary violation detection
  std::optional<AllTypeVariant> min_rhs;
  std::optional<AllTypeVariant> max_rhs;

  int right_neighbor_flag = 0;
  int right_neighbor_flag_contribution = 0;
  int local_violation_count_contribution = 0;

  /**
   * Compute local violation count based on dependency type.
   * FD: distinct_rhs_count - 1 (same LHS with different RHS = violation)
   * OD: distinct_rhs_count - 1 (same LHS with different RHS = ambiguous ordering = violation)
   *     PLUS boundary violations tracked separately via right_neighbor_flag
   */
  int local_violation_count(DependencyType dep_type) const {
    // Both FD and OD: distinct RHS values - 1 (same LHS with different RHS = violation)
    // For OD, this captures the "ambiguous ordering" case
    return rhs_values.empty() ? 0 : static_cast<int>(rhs_values.size()) - 1;
  }

  // Legacy method for backward compatibility - returns count-based violations
  // This is used by tests that expect the old behavior (before FD/OD distinction)
  int local_violation_count() const {
    // Use count-based calculation for legacy compatibility
    return static_cast<int>(count) > 0 ? static_cast<int>(count) - 1 : 0;
  }
};

/**
 * Key-value entry stored in B-Tree nodes.
 */
struct BTreeEntry {
  std::vector<AllTypeVariant> key;
  std::shared_ptr<BTreeValue> value;
};

/**
 * B-Tree node supporting both internal and leaf nodes.
 * Minimum degree t=3 (2t-1=5 max keys per node).
 *
 * Leaf nodes maintain left/right neighbor pointers for efficient
 * neighbor flag updates during dependency validation.
 */
class BTreeNode : public std::enable_shared_from_this<BTreeNode> {
 public:
  explicit BTreeNode(int t, bool leaf);

  std::vector<BTreeEntry> entries;
  std::vector<std::shared_ptr<BTreeNode>> children;
  int t;
  bool leaf;

  std::weak_ptr<BTreeNode> left_neighbor;
  std::weak_ptr<BTreeNode> right_neighbor;

  void insert_non_full(const std::vector<AllTypeVariant>& key, std::shared_ptr<BTreeValue> value);
  void split_child(int i, std::shared_ptr<BTreeNode> y);
  std::shared_ptr<BTreeValue> search(const std::vector<AllTypeVariant>& key) const;
  std::shared_ptr<BTreeValue> lower_bound(const std::vector<AllTypeVariant>& key) const;
  std::shared_ptr<BTreeValue> upper_bound(const std::vector<AllTypeVariant>& key) const;

  /** Finds the leaf containing a key, optionally returning position within leaf. */
  std::shared_ptr<BTreeNode> find_leaf(const std::vector<AllTypeVariant>& key, int* position = nullptr) const;

  std::vector<AllTypeVariant> get_min_key() const;
  std::vector<AllTypeVariant> get_max_key() const;

  bool is_smallest_in_leaf(const std::vector<AllTypeVariant>& key) const;
  bool is_largest_in_leaf(const std::vector<AllTypeVariant>& key) const;

  BTreeEntry* get_entry_at(int position);
  const BTreeEntry* get_entry_at(int position) const;

  std::shared_ptr<BTreeNode> get_leftmost_leaf();
  std::shared_ptr<BTreeNode> get_rightmost_leaf();
};

}  // namespace hyrise
