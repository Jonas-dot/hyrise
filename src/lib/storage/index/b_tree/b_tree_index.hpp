#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "storage/index/abstract_chunk_index.hpp"
#include "storage/index/b_tree/b_tree_nodes.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class BTreeIndexTest;

/**
 * B-Tree index with support for Functional Dependency (FD) and Order Dependency (OD) validation.
 *
 * This index extends the standard B-Tree with metadata tracking for incremental dependency validation:
 * - Per-entry metadata tracks local violations (duplicates) and neighbor flags
 * - A global violation counter provides O(1) dependency status checks
 * - Leaf nodes are linked for efficient neighbor access during flag updates
 *
 * The validation algorithm uses delta-based updates: operations return MetadataDeltas indicating
 * how the global violation count changed, avoiding full rescans.
 *
 * Find more information in: src/lib/storage/index/b_tree/B_TREE_INDEX.md
 */
class BTreeIndex : public AbstractChunkIndex {
  friend class BTreeIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See AbstractChunkIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  BTreeIndex() = delete;
  BTreeIndex(const BTreeIndex&) = delete;
  BTreeIndex& operator=(const BTreeIndex&) = delete;
  BTreeIndex(BTreeIndex&&) = default;

  explicit BTreeIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index);

  ChunkIndexType type() const;

  /**
   * Global violation counter tracking total violations across all entries.
   * Sum of all right_neighbor_flags plus all local_violation_counts.
   * If zero, the dependency likely holds; if positive, violations exist.
   */
  int global_violation_count = 0;

  /**
   * The type of dependency this index is validating.
   * - FD: Functional Dependency - same LHS must have same RHS
   * - OD: Order Dependency - LHS ordering implies RHS ordering
   */
  DependencyType dependency_type = DependencyType::FD;

  /**
   * Processes an insert operation for dependency validation.
   *
   * For FD validation:
   * - Tracks distinct RHS values per LHS key
   * - local_violations = distinct_rhs_count - 1 (same LHS with different RHS = violation)
   * - right_neighbor_flag: not used (FD only cares about same-key relationships)
   *
   * For OD validation:
   * - Tracks distinct RHS values AND min/max RHS values per LHS key
   * - local_violations = distinct_rhs_count - 1 (same LHS with different RHS = ambiguous ordering)
   * - right_neighbor_flag: 1 if max_rhs > min_rhs of next key (boundary violation)
   *
   * Total OD violations = sum(local_violations) + sum(boundary_flags)
   *
   * @param left_key Key from left side of dependency (LHS)
   * @param right_key Key from right side of dependency (RHS value being inserted)
   * @param dep_type Type of dependency (FD or OD)
   * @return MetadataDeltas containing flag_delta and local_violation_count_delta
   */
  MetadataDeltas insert_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                             const std::vector<AllTypeVariant>& right_key, DependencyType dep_type);

  /**
   * Processes a delete operation for dependency validation.
   *
   * For FD validation:
   * - Removes RHS value from the set for this LHS key
   * - If rhs_values becomes empty, removes the entry entirely
   * - Updates local_violation_count accordingly
   *
   * For OD validation:
   * - Removes RHS value from the set and recomputes min/max
   * - If rhs_values becomes empty, removes the entry entirely
   * - Updates local_violation_count and right_neighbor_flag
   *
   * @param left_key Key from left side of dependency (LHS)
   * @param right_key Key from right side of dependency (RHS value being deleted)
   * @param dep_type Type of dependency (FD or OD)
   * @return MetadataDeltas containing flag_delta and local_violation_count_delta
   */
  MetadataDeltas delete_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                             const std::vector<AllTypeVariant>& right_key, DependencyType dep_type);

  /**
   * Processes an update operation (equivalent to delete + insert).
   */
  MetadataDeltas update_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                             const std::vector<AllTypeVariant>& old_right_key,
                                             const std::vector<AllTypeVariant>& new_right_key, DependencyType dep_type);

  /**
   * Low-level flag operations for direct metadata manipulation.
   */
  void set_right_neighbor_flag(const std::vector<AllTypeVariant>& key, int flag);
  int get_right_neighbor_flag(const std::vector<AllTypeVariant>& key) const;
  void recompute_local_violation_delta(const std::vector<AllTypeVariant>& key, DependencyType dep_type);

  /** Returns the metadata value for a key, or nullptr if not found. */
  std::shared_ptr<BTreeValue> get_value(const std::vector<AllTypeVariant>& key) const;

  /** Returns the maximum key from the left neighbor leaf, or empty vector if none. */
  std::vector<AllTypeVariant> get_left_neighbor_max_key(const std::vector<AllTypeVariant>& key) const;

  /**
   * Dynamically inserts a key into the index.
   * If the key already exists, increments its count.
   * If the key is new, creates a new entry in the B-Tree.
   * This enables true online validation where keys can be added after index construction.
   *
   * @param key The key to insert
   * @return true if a new entry was created, false if count was incremented
   */
  bool insert_key(const std::vector<AllTypeVariant>& key);

  /**
   * Dynamically removes a key from the index.
   * If count > 1, decrements the count.
   * If count == 1, removes the entry from the B-Tree entirely.
   * This enables true online validation where keys can be removed after index construction.
   *
   * @param key The key to remove
   * @return true if the entry was completely removed, false if count was decremented
   */
  bool remove_key(const std::vector<AllTypeVariant>& key);

  /** Returns true if the key exists in the index. */
  bool contains_key(const std::vector<AllTypeVariant>& key) const;

  /** Returns the number of distinct keys in the index. */
  size_t key_count() const;

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _cbegin() const final;
  Iterator _cend() const final;
  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const final;
  size_t _memory_consumption() const final;

  void _insert(const std::vector<AllTypeVariant>& key, std::shared_ptr<BTreeValue> value);
  void _remove_entry(const std::vector<AllTypeVariant>& key);
  void _link_leaf_neighbors();
  int _compute_right_neighbor_flag(
      std::shared_ptr<BTreeNode> leaf, int position,
      std::function<bool(const std::vector<AllTypeVariant>&, const std::vector<AllTypeVariant>&)> check_violation_func);

  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;
  std::vector<ChunkOffset> _chunk_offsets;
  std::shared_ptr<BTreeNode> _root;
};

}  // namespace hyrise
