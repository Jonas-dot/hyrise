#include "storage/index/b_tree/b_tree_index.hpp"

#include <algorithm>
#include <functional>
#include <numeric>

#include "storage/abstract_segment.hpp"

namespace hyrise {

BTreeIndex::BTreeIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index)
    : AbstractChunkIndex(ChunkIndexType::BTree), _indexed_segments(segments_to_index) {
  Assert(!_indexed_segments.empty(), "BTreeIndex requires at least one segment.");

  auto chunk_size = _indexed_segments[0]->size();

  std::vector<ChunkOffset> offsets(chunk_size);
  std::iota(offsets.begin(), offsets.end(), ChunkOffset{0});

  std::sort(offsets.begin(), offsets.end(), [&](ChunkOffset a, ChunkOffset b) {
    for (const auto& segment : _indexed_segments) {
      auto val_a = (*segment)[a];
      auto val_b = (*segment)[b];

      if (variant_is_null(val_a) && variant_is_null(val_b))
        continue;
      if (variant_is_null(val_a))
        return false;
      if (variant_is_null(val_b))
        return true;

      if (val_a < val_b)
        return true;
      if (val_b < val_a)
        return false;
    }
    return false;
  });

  _chunk_offsets.reserve(chunk_size);

  int t = 3;
  _root = std::make_shared<BTreeNode>(t, true);

  std::vector<AllTypeVariant> current_key;
  ChunkOffset start_index{0};
  ChunkOffset count{0};
  bool first = true;

  for (auto offset : offsets) {
    std::vector<AllTypeVariant> key;
    bool is_null = false;
    for (const auto& segment : _indexed_segments) {
      auto val = (*segment)[offset];
      if (variant_is_null(val)) {
        is_null = true;
        break;
      }
      key.push_back(val);
    }

    if (is_null) {
      _null_positions.push_back(offset);
      continue;
    }

    if (first) {
      current_key = key;
      start_index = ChunkOffset{static_cast<ChunkOffset::base_type>(_chunk_offsets.size())};
      count = ChunkOffset{0};
      first = false;
    } else if (key != current_key) {
      auto value = std::make_shared<BTreeValue>();
      value->start_index = start_index;
      value->count = count;
      _insert(current_key, value);

      current_key = key;
      start_index = ChunkOffset{static_cast<ChunkOffset::base_type>(_chunk_offsets.size())};
      count = ChunkOffset{0};
    }

    _chunk_offsets.push_back(offset);
    count++;
  }

  if (!first) {
    auto value = std::make_shared<BTreeValue>();
    value->start_index = start_index;
    value->count = count;
    _insert(current_key, value);
  }

  // Link leaf neighbors after tree construction
  _link_leaf_neighbors();
}

void BTreeIndex::_link_leaf_neighbors() {
  if (!_root)
    return;

  // Get leftmost leaf and traverse through leaves
  auto current = _root->get_leftmost_leaf();
  std::shared_ptr<BTreeNode> prev = nullptr;

  // We need to traverse all leaves and link them
  // Since we don't have a next pointer yet, we collect all leaves first
  std::vector<std::shared_ptr<BTreeNode>> leaves;

  std::function<void(std::shared_ptr<BTreeNode>)> collect_leaves = [&](std::shared_ptr<BTreeNode> node) {
    if (node->leaf) {
      leaves.push_back(node);
    } else {
      for (auto& child : node->children) {
        collect_leaves(child);
      }
    }
  };

  collect_leaves(_root);

  // Link them
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0) {
      leaves[i]->left_neighbor = leaves[i - 1];
    }
    if (i < leaves.size() - 1) {
      leaves[i]->right_neighbor = leaves[i + 1];
    }
  }
}

void BTreeIndex::_insert(const std::vector<AllTypeVariant>& key, std::shared_ptr<BTreeValue> value) {
  if (_root->entries.size() == static_cast<size_t>(2 * _root->t - 1)) {
    auto s = std::make_shared<BTreeNode>(_root->t, false);
    s->children.push_back(_root);
    s->split_child(0, _root);
    int i = 0;
    if (s->entries[0].key < key) {
      i++;
    }
    s->children[i]->insert_non_full(key, value);
    _root = s;
  } else {
    _root->insert_non_full(key, value);
  }
}

ChunkIndexType BTreeIndex::type() const {
  return ChunkIndexType::BTree;
}

BTreeIndex::Iterator BTreeIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  auto result = _root->lower_bound(values);
  if (!result) {
    return _chunk_offsets.cend();
  }
  return _chunk_offsets.cbegin() + result->start_index;
}

BTreeIndex::Iterator BTreeIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  auto result = _root->upper_bound(values);
  if (!result) {
    return _chunk_offsets.cend();
  }
  return _chunk_offsets.cbegin() + result->start_index;
}

BTreeIndex::Iterator BTreeIndex::_cbegin() const {
  return _chunk_offsets.cbegin();
}

BTreeIndex::Iterator BTreeIndex::_cend() const {
  return _chunk_offsets.cend();
}

std::vector<std::shared_ptr<const AbstractSegment>> BTreeIndex::_get_indexed_segments() const {
  return _indexed_segments;
}

size_t BTreeIndex::_memory_consumption() const {
  size_t size = sizeof(BTreeIndex);
  size += _chunk_offsets.capacity() * sizeof(ChunkOffset);
  size += _null_positions.capacity() * sizeof(ChunkOffset);
  return size;
}

size_t BTreeIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                               uint32_t value_bytes) {
  // Estimate B-Tree memory: nodes + entries + chunk offsets
  // Each entry has a key and metadata (BTreeValue)
  const size_t entry_size = value_bytes + sizeof(BTreeValue);
  const size_t node_overhead = sizeof(BTreeNode);

  // Approximate number of nodes: distinct_count / (2 * t - 1) where t=3
  const size_t entries_per_node = 5;  // 2 * t - 1
  const size_t num_nodes = (distinct_count + entries_per_node - 1) / entries_per_node;

  return num_nodes * (node_overhead + entries_per_node * entry_size) + row_count * sizeof(ChunkOffset);
}

bool BTreeIndex::insert_key(const std::vector<AllTypeVariant>& key) {
  // Check if key already exists
  auto existing_value = _root->search(key);
  if (existing_value) {
    // Key exists, increment count
    existing_value->count++;
    return false;
  }

  // Key doesn't exist, create new entry
  auto new_value = std::make_shared<BTreeValue>();
  new_value->start_index = ChunkOffset{0};  // Will be updated if needed for iteration
  new_value->count = ChunkOffset{1};
  new_value->right_neighbor_flag = 0;
  new_value->right_neighbor_flag_contribution = 0;
  new_value->local_violation_count_contribution = 0;

  _insert(key, new_value);

  // Re-link leaf neighbors after insertion (structure may have changed due to splits)
  _link_leaf_neighbors();

  return true;
}

bool BTreeIndex::remove_key(const std::vector<AllTypeVariant>& key) {
  auto existing_value = _root->search(key);
  if (!existing_value) {
    return false;  // Key doesn't exist
  }

  if (existing_value->count > ChunkOffset{1}) {
    // Decrement count
    existing_value->count--;
    return false;
  }

  // count == 1, need to remove the entry entirely
  _remove_entry(key);

  // Re-link leaf neighbors after removal
  _link_leaf_neighbors();

  return true;
}

void BTreeIndex::_remove_entry(const std::vector<AllTypeVariant>& key) {
  // Find and remove the entry from the tree
  // This is a simplified removal that works by finding the entry anywhere in the tree
  std::function<bool(std::shared_ptr<BTreeNode>)> remove_from_node = [&](std::shared_ptr<BTreeNode> node) -> bool {
    // Search for the key in this node's entries
    for (size_t i = 0; i < node->entries.size(); ++i) {
      if (node->entries[i].key == key) {
        // Found it - remove from this node
        node->entries.erase(node->entries.begin() + static_cast<int>(i));
        return true;
      }
    }

    // If not a leaf, recurse into appropriate child
    if (!node->leaf) {
      size_t i = 0;
      while (i < node->entries.size() && key > node->entries[i].key) {
        i++;
      }
      if (i < node->children.size()) {
        return remove_from_node(node->children[i]);
      }
    }

    return false;  // Key not found
  };

  remove_from_node(_root);

  // Note: This simplified removal doesn't handle B-Tree rebalancing.
  // For the purpose of FD/OD validation tracking, this works as long as
  // we don't rely on strict B-Tree balance properties for correctness.
}

bool BTreeIndex::contains_key(const std::vector<AllTypeVariant>& key) const {
  return _root->search(key) != nullptr;
}

size_t BTreeIndex::key_count() const {
  size_t count = 0;
  std::function<void(std::shared_ptr<BTreeNode>)> count_keys = [&](std::shared_ptr<BTreeNode> node) {
    // Count entries in this node (B-Tree stores data in both internal and leaf nodes)
    count += node->entries.size();
    // Recurse into children if not a leaf
    if (!node->leaf) {
      for (auto& child : node->children) {
        count_keys(child);
      }
    }
  };
  count_keys(_root);
  return count;
}

void BTreeIndex::set_right_neighbor_flag(const std::vector<AllTypeVariant>& key, int flag) {
  auto value = _root->search(key);
  if (value) {
    // Remove old contribution from global count
    global_violation_count -= value->right_neighbor_flag_contribution;
    // Set new flag value
    value->right_neighbor_flag = flag;
    // Add new contribution to global count
    value->right_neighbor_flag_contribution = flag;
    global_violation_count += value->right_neighbor_flag_contribution;
  }
}

int BTreeIndex::get_right_neighbor_flag(const std::vector<AllTypeVariant>& key) const {
  auto value = _root->search(key);
  if (value) {
    return value->right_neighbor_flag;
  }
  return 0;
}

void BTreeIndex::recompute_local_violation_delta(const std::vector<AllTypeVariant>& key, DependencyType dep_type) {
  auto value = _root->search(key);
  if (value) {
    // Remove old contribution from global count
    global_violation_count -= value->local_violation_count_contribution;
    // Compute current local violation count based on dependency type
    int current_local = value->local_violation_count(dep_type);
    // Update contribution
    value->local_violation_count_contribution = current_local;
    // Add new contribution to global count
    global_violation_count += current_local;
  }
}

std::shared_ptr<BTreeValue> BTreeIndex::get_value(const std::vector<AllTypeVariant>& key) const {
  return _root->search(key);
}

std::vector<AllTypeVariant> BTreeIndex::get_left_neighbor_max_key(const std::vector<AllTypeVariant>& key) const {
  int position = -1;
  auto leaf = _root->find_leaf(key, &position);

  if (!leaf) {
    return {};
  }

  // If key is at position 0, need to check left neighbor leaf
  if (position == 0 || position == -1) {
    if (auto left_leaf = leaf->left_neighbor.lock()) {
      return left_leaf->get_max_key();
    }
    return {};
  }

  // Otherwise, left neighbor is at position - 1 in same leaf
  if (position > 0 && position <= static_cast<int>(leaf->entries.size())) {
    return leaf->entries[position - 1].key;
  }

  return {};
}

int BTreeIndex::_compute_right_neighbor_flag(
    std::shared_ptr<BTreeNode> leaf, int position,
    std::function<bool(const std::vector<AllTypeVariant>&, const std::vector<AllTypeVariant>&)> check_violation_func) {
  if (!leaf || position < 0 || position >= static_cast<int>(leaf->entries.size())) {
    return 0;
  }

  const auto& current_key = leaf->entries[position].key;

  // Get right neighbor's key
  std::vector<AllTypeVariant> right_key;

  if (position < static_cast<int>(leaf->entries.size()) - 1) {
    // Right neighbor is in same leaf
    right_key = leaf->entries[position + 1].key;
  } else {
    // Right neighbor is in next leaf
    if (auto right_leaf = leaf->right_neighbor.lock()) {
      if (!right_leaf->entries.empty()) {
        right_key = right_leaf->entries[0].key;
      }
    }
  }

  if (right_key.empty()) {
    return 0;  // No right neighbor
  }

  // Check if there's a violation
  return check_violation_func(current_key, right_key) ? 1 : 0;
}

// Helper to compute OD violation: max_rhs of current > min_rhs of right neighbor
int compute_od_boundary_flag(const std::shared_ptr<BTreeValue>& current_value,
                             const std::shared_ptr<BTreeValue>& right_value) {
  if (!current_value || !right_value) {
    return 0;
  }
  if (!current_value->max_rhs.has_value() || !right_value->min_rhs.has_value()) {
    return 0;
  }
  // OD violation: if max_rhs of current LHS > min_rhs of next LHS
  return (current_value->max_rhs.value() > right_value->min_rhs.value()) ? 1 : 0;
}

MetadataDeltas BTreeIndex::insert_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                                       const std::vector<AllTypeVariant>& right_key,
                                                       DependencyType dep_type) {
  MetadataDeltas deltas;

  // First, ensure the LHS key exists in the index
  // For FD, don't increment count - we track by rhs_values
  // For OD, we still use count
  auto existing_value = _root->search(left_key);
  if (!existing_value) {
    // Key doesn't exist, create new entry
    auto new_value = std::make_shared<BTreeValue>();
    new_value->start_index = ChunkOffset{0};
    new_value->count = (dep_type == DependencyType::OD) ? ChunkOffset{1} : ChunkOffset{0};
    new_value->right_neighbor_flag = 0;
    new_value->right_neighbor_flag_contribution = 0;
    new_value->local_violation_count_contribution = 0;

    _insert(left_key, new_value);
    _link_leaf_neighbors();
  } else if (dep_type == DependencyType::OD) {
    // For OD, increment count as before
    existing_value->count++;
  }
  // For FD, we don't change count - it's tracked via rhs_values

  // Access leaf via left side of dependency
  int position = -1;
  auto leaf = _root->find_leaf(left_key, &position);

  if (!leaf || position < 0) {
    return deltas;  // Should not happen after insert_key
  }

  auto value = leaf->entries[position].value;
  if (!value) {
    return deltas;
  }

  // Save old values for delta computation
  int old_flag_contribution = value->right_neighbor_flag_contribution;
  int old_local_contribution = value->local_violation_count_contribution;

  // Update RHS tracking based on dependency type
  if (dep_type == DependencyType::FD) {
    // FD: Track distinct RHS values
    // For composite RHS, we use the first element (simplification)
    // For full support, we'd need a proper hash for vector<AllTypeVariant>
    if (!right_key.empty()) {
      value->rhs_values.insert(right_key[0]);
    }

    // Compute local violation delta
    // local_violations = distinct_rhs_count - 1
    int new_local = value->local_violation_count(dep_type);
    value->local_violation_count_contribution = new_local;
    deltas.local_violation_count_delta = new_local - old_local_contribution;

    // FD doesn't use neighbor flags (violations are only within same LHS)
    // Clear any existing flag contribution
    if (value->right_neighbor_flag_contribution != 0) {
      deltas.flag_delta -= value->right_neighbor_flag_contribution;
      value->right_neighbor_flag = 0;
      value->right_neighbor_flag_contribution = 0;
    }

  } else {
    // OD: Track both distinct RHS values (for local violations) and min/max (for boundary violations)
    if (!right_key.empty()) {
      const auto& rhs = right_key[0];

      // Track distinct RHS values for local violation detection
      value->rhs_values.insert(rhs);

      // Update min
      if (!value->min_rhs.has_value() || rhs < value->min_rhs.value()) {
        value->min_rhs = rhs;
      }
      // Update max
      if (!value->max_rhs.has_value() || rhs > value->max_rhs.value()) {
        value->max_rhs = rhs;
      }
    }

    // OD: local_violations = distinct_rhs_count - 1 (same LHS with different RHS = ambiguous ordering)
    int new_local = value->local_violation_count(dep_type);
    value->local_violation_count_contribution = new_local;
    deltas.local_violation_count_delta = new_local - old_local_contribution;

    // OD: Always update this entry's right neighbor flag since max_rhs may have changed
    int new_flag = 0;
    if (position + 1 < static_cast<int>(leaf->entries.size())) {
      // Right neighbor is in same leaf
      new_flag = compute_od_boundary_flag(value, leaf->entries[position + 1].value);
    } else if (auto right_leaf = leaf->right_neighbor.lock()) {
      // Right neighbor is in next leaf
      if (!right_leaf->entries.empty()) {
        new_flag = compute_od_boundary_flag(value, right_leaf->entries[0].value);
      }
    }
    value->right_neighbor_flag = new_flag;
    value->right_neighbor_flag_contribution = new_flag;
    deltas.flag_delta += (new_flag - old_flag_contribution);

    // Also update predecessor's flag since our min_rhs may have changed
    // Predecessor is the entry with the next smaller key
    if (position > 0) {
      // Predecessor is in same leaf
      auto& pred_entry = leaf->entries[position - 1];
      if (pred_entry.value) {
        int old_pred_flag = pred_entry.value->right_neighbor_flag_contribution;
        int new_pred_flag = compute_od_boundary_flag(pred_entry.value, value);
        pred_entry.value->right_neighbor_flag = new_pred_flag;
        pred_entry.value->right_neighbor_flag_contribution = new_pred_flag;
        deltas.flag_delta += (new_pred_flag - old_pred_flag);
      }
    } else {
      // Predecessor is in left neighbor leaf (if exists)
      if (auto left_leaf = leaf->left_neighbor.lock()) {
        if (!left_leaf->entries.empty()) {
          int last_pos = static_cast<int>(left_leaf->entries.size()) - 1;
          auto& left_entry = left_leaf->entries[last_pos];
          if (left_entry.value) {
            int old_left_flag = left_entry.value->right_neighbor_flag_contribution;
            int new_left_flag = compute_od_boundary_flag(left_entry.value, value);
            left_entry.value->right_neighbor_flag = new_left_flag;
            left_entry.value->right_neighbor_flag_contribution = new_left_flag;
            deltas.flag_delta += (new_left_flag - old_left_flag);
          }
        }
      }
    }
  }

  // Update global_violation_count
  global_violation_count += deltas.flag_delta + deltas.local_violation_count_delta;

  return deltas;
}

MetadataDeltas BTreeIndex::delete_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                                       const std::vector<AllTypeVariant>& right_key,
                                                       DependencyType dep_type) {
  MetadataDeltas deltas;

  // Access leaf via left side of dependency
  int position = -1;
  auto leaf = _root->find_leaf(left_key, &position);

  if (!leaf || position < 0) {
    return deltas;  // Key not found
  }

  auto value = leaf->entries[position].value;
  if (!value) {
    return deltas;
  }

  // Save old values for delta computation
  int old_flag_contribution = value->right_neighbor_flag_contribution;
  int old_local_contribution = value->local_violation_count_contribution;

  bool is_smallest = leaf->is_smallest_in_leaf(left_key);
  bool is_largest = leaf->is_largest_in_leaf(left_key);

  if (dep_type == DependencyType::FD) {
    // FD: Remove RHS value from the set
    // Note: This assumes we're tracking multi-set semantics. For proper FD validation,
    // we'd need to track count per RHS value. For simplicity, we keep the set approach
    // which works if each (LHS, RHS) pair is unique.
    if (!right_key.empty()) {
      value->rhs_values.erase(right_key[0]);
    }

    // Check if this LHS should be completely removed
    if (value->rhs_values.empty()) {
      // Remove contributions from global count
      deltas.flag_delta -= old_flag_contribution;
      deltas.local_violation_count_delta -= old_local_contribution;

      // Directly remove the key entry (bypass count check in remove_key)
      _remove_entry(left_key);
      _link_leaf_neighbors();
    } else {
      // Update local violation count
      int new_local = value->local_violation_count(dep_type);
      value->local_violation_count_contribution = new_local;
      deltas.local_violation_count_delta = new_local - old_local_contribution;
    }

  } else {
    // OD: Remove RHS value from the set (similar to FD, but also track min/max)
    if (!right_key.empty()) {
      value->rhs_values.erase(right_key[0]);
    }

    // Check if this LHS should be completely removed
    if (value->rhs_values.empty()) {
      // Update left neighbor's flag if needed
      if (is_smallest && position == 0) {
        if (auto left_leaf = leaf->left_neighbor.lock()) {
          if (!left_leaf->entries.empty()) {
            int last_pos = static_cast<int>(left_leaf->entries.size()) - 1;
            auto& left_entry = left_leaf->entries[last_pos];
            if (left_entry.value) {
              int old_left_flag = left_entry.value->right_neighbor_flag_contribution;
              int new_flag = 0;
              // After deletion, left neighbor's right neighbor becomes our successor
              if (leaf->entries.size() > 1) {
                new_flag = compute_od_boundary_flag(left_entry.value, leaf->entries[1].value);
              } else if (auto right_leaf = leaf->right_neighbor.lock()) {
                if (!right_leaf->entries.empty()) {
                  new_flag = compute_od_boundary_flag(left_entry.value, right_leaf->entries[0].value);
                }
              }
              left_entry.value->right_neighbor_flag = new_flag;
              left_entry.value->right_neighbor_flag_contribution = new_flag;
              deltas.flag_delta += (new_flag - old_left_flag);
            }
          }
        }
      }

      // If entry was largest, update predecessor's flag
      if (is_largest && position > 0) {
        auto& prev_entry = leaf->entries[position - 1];
        if (prev_entry.value) {
          int old_prev_flag = prev_entry.value->right_neighbor_flag_contribution;
          int new_flag = 0;
          if (auto right_leaf = leaf->right_neighbor.lock()) {
            if (!right_leaf->entries.empty()) {
              new_flag = compute_od_boundary_flag(prev_entry.value, right_leaf->entries[0].value);
            }
          }
          prev_entry.value->right_neighbor_flag = new_flag;
          prev_entry.value->right_neighbor_flag_contribution = new_flag;
          deltas.flag_delta += (new_flag - old_prev_flag);
        }
      }

      // Remove this entry's contributions
      deltas.flag_delta -= old_flag_contribution;
      deltas.local_violation_count_delta -= old_local_contribution;

      // Directly remove the key entry (bypass count check)
      _remove_entry(left_key);
      _link_leaf_neighbors();

    } else {
      // Entry stays, update min/max and local violations
      // Recompute min/max from remaining rhs_values
      value->min_rhs = std::nullopt;
      value->max_rhs = std::nullopt;
      for (const auto& rhs : value->rhs_values) {
        if (!value->min_rhs.has_value() || rhs < value->min_rhs.value()) {
          value->min_rhs = rhs;
        }
        if (!value->max_rhs.has_value() || rhs > value->max_rhs.value()) {
          value->max_rhs = rhs;
        }
      }

      // Update boundary flag
      int new_flag = 0;
      if (position + 1 < static_cast<int>(leaf->entries.size())) {
        new_flag = compute_od_boundary_flag(value, leaf->entries[position + 1].value);
      } else if (auto right_leaf = leaf->right_neighbor.lock()) {
        if (!right_leaf->entries.empty()) {
          new_flag = compute_od_boundary_flag(value, right_leaf->entries[0].value);
        }
      }
      value->right_neighbor_flag = new_flag;
      value->right_neighbor_flag_contribution = new_flag;
      deltas.flag_delta += (new_flag - old_flag_contribution);

      // Update local violations (distinct_rhs_count - 1)
      int new_local = value->local_violation_count(dep_type);
      value->local_violation_count_contribution = new_local;
      deltas.local_violation_count_delta = new_local - old_local_contribution;
    }
  }

  // Update global_violation_count
  global_violation_count += deltas.flag_delta + deltas.local_violation_count_delta;

  return deltas;
}

MetadataDeltas BTreeIndex::update_entry_for_validation(const std::vector<AllTypeVariant>& left_key,
                                                       const std::vector<AllTypeVariant>& old_right_key,
                                                       const std::vector<AllTypeVariant>& new_right_key,
                                                       DependencyType dep_type) {
  // Update = Delete + Insert
  MetadataDeltas delete_deltas = delete_entry_for_validation(left_key, old_right_key, dep_type);
  MetadataDeltas insert_deltas = insert_entry_for_validation(left_key, new_right_key, dep_type);

  MetadataDeltas total;
  total.flag_delta = delete_deltas.flag_delta + insert_deltas.flag_delta;
  total.local_violation_count_delta =
      delete_deltas.local_violation_count_delta + insert_deltas.local_violation_count_delta;

  return total;
}

}  // namespace hyrise
