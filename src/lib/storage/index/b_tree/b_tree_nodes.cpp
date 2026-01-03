#include "b_tree_nodes.hpp"

#include <algorithm>
#include <array>

namespace hyrise {

BTreeNode::BTreeNode(int t, bool leaf) : t(t), leaf(leaf) {
  entries.reserve(2 * t - 1);
  children.reserve(2 * t);
  hints.fill(0);  // Initialize hint array
}

void BTreeNode::insert_non_full(const std::vector<AllTypeVariant>& key, std::shared_ptr<BTreeValue> value) {
  auto i = static_cast<int>(entries.size()) - 1;

  // Compute key head for optimization (TU-Munich)
  uint32_t key_head = BTreeEntry::compute_head(key);

  if (leaf) {
    while (i >= 0 && key < entries[i].key) {
      i--;
    }
    BTreeEntry new_entry;
    new_entry.key = key;
    new_entry.value = value;
    new_entry.key_head = key_head;
    entries.insert(entries.begin() + i + 1, new_entry);
    update_hint(static_cast<size_t>(i + 1));  // TU-Munich: Update hint after insert
  } else {
    while (i >= 0 && key < entries[i].key) {
      i--;
    }
    i++;
    if (children[i]->entries.size() == static_cast<size_t>(2 * t - 1)) {
      split_child(i, children[i]);
      if (key > entries[i].key) {
        i++;
      }
    }
    children[i]->insert_non_full(key, value);
  }
}

void BTreeNode::split_child(int i, std::shared_ptr<BTreeNode> y) {
  auto z = std::make_shared<BTreeNode>(y->t, y->leaf);

  z->entries.assign(y->entries.begin() + t, y->entries.end());

  if (!y->leaf) {
    z->children.assign(y->children.begin() + t, y->children.end());
    y->children.resize(t);
  }

  auto mid_entry = y->entries[t - 1];

  y->entries.resize(t - 1);

  children.insert(children.begin() + i + 1, z);
  entries.insert(entries.begin() + i, mid_entry);

  // Update neighbor pointers for leaf nodes
  if (y->leaf) {
    // z is the new right sibling of y
    z->left_neighbor = y;
    z->right_neighbor = y->right_neighbor;

    // Update y's old right neighbor to point to z
    if (auto old_right = y->right_neighbor.lock()) {
      old_right->left_neighbor = z;
    }

    // y's right neighbor is now z
    y->right_neighbor = z;
  }

  // TU-Munich: Rebuild hints after structural change
  y->make_hints();
  z->make_hints();
  make_hints();
}

std::shared_ptr<BTreeValue> BTreeNode::search(const std::vector<AllTypeVariant>& key) const {
  // TU-Munich optimization: Use hint-based optimized lower bound
  bool found = false;
  size_t pos = lower_bound_optimized(key, found);

  if (found && pos < entries.size()) {
    return entries[pos].value;
  }
  if (leaf) {
    return nullptr;
  }
  // For inner nodes, navigate to the correct child
  // pos is the first entry >= key, so we go to children[pos]
  if (pos < children.size()) {
    return children[pos]->search(key);
  }
  return nullptr;
}

std::shared_ptr<BTreeValue> BTreeNode::lower_bound(const std::vector<AllTypeVariant>& key) const {
  size_t i = 0;
  while (i < entries.size() && key > entries[i].key) {
    i++;
  }

  if (i < entries.size() && key == entries[i].key) {
    return entries[i].value;
  }

  if (leaf) {
    if (i < entries.size()) {
      return entries[i].value;
    } else {
      return nullptr;
    }
  }

  auto result = children[i]->lower_bound(key);
  if (result) {
    return result;
  }

  if (i < entries.size()) {
    return entries[i].value;
  }

  return nullptr;
}

std::shared_ptr<BTreeValue> BTreeNode::upper_bound(const std::vector<AllTypeVariant>& key) const {
  size_t i = 0;
  while (i < entries.size() && key >= entries[i].key) {
    i++;
  }

  if (leaf) {
    if (i < entries.size()) {
      return entries[i].value;
    } else {
      return nullptr;
    }
  }

  auto result = children[i]->upper_bound(key);
  if (result) {
    return result;
  }

  if (i < entries.size()) {
    return entries[i].value;
  }

  return nullptr;
}

std::shared_ptr<BTreeNode> BTreeNode::find_leaf(const std::vector<AllTypeVariant>& key, int* position) const {
  size_t i = 0;
  while (i < entries.size() && key > entries[i].key) {
    i++;
  }

  if (leaf) {
    if (position && i < entries.size() && key == entries[i].key) {
      *position = static_cast<int>(i);
    } else if (position) {
      *position = -1;  // Key not found
    }
    return std::const_pointer_cast<BTreeNode>(shared_from_this());
  }

  return children[i]->find_leaf(key, position);
}

std::vector<AllTypeVariant> BTreeNode::get_min_key() const {
  if (leaf) {
    if (entries.empty()) {
      return {};
    }
    return entries.front().key;
  }
  return children.front()->get_min_key();
}

std::vector<AllTypeVariant> BTreeNode::get_max_key() const {
  if (leaf) {
    if (entries.empty()) {
      return {};
    }
    return entries.back().key;
  }
  return children.back()->get_max_key();
}

bool BTreeNode::is_smallest_in_leaf(const std::vector<AllTypeVariant>& key) const {
  if (!leaf || entries.empty()) {
    return false;
  }
  return entries.front().key == key;
}

bool BTreeNode::is_largest_in_leaf(const std::vector<AllTypeVariant>& key) const {
  if (!leaf || entries.empty()) {
    return false;
  }
  return entries.back().key == key;
}

BTreeEntry* BTreeNode::get_entry_at(int position) {
  if (position >= 0 && static_cast<size_t>(position) < entries.size()) {
    return &entries[position];
  }
  return nullptr;
}

const BTreeEntry* BTreeNode::get_entry_at(int position) const {
  if (position >= 0 && static_cast<size_t>(position) < entries.size()) {
    return &entries[position];
  }
  return nullptr;
}

std::shared_ptr<BTreeNode> BTreeNode::get_leftmost_leaf() {
  if (leaf) {
    return shared_from_this();
  }
  return children.front()->get_leftmost_leaf();
}

std::shared_ptr<BTreeNode> BTreeNode::get_rightmost_leaf() {
  if (leaf) {
    return shared_from_this();
  }
  return children.back()->get_rightmost_leaf();
}

// ============================================================================
// TU-Munich B-Tree Optimizations
// Based on "B-Trees Are Back: Engineering Fast and Pageable Node Layouts"
// ============================================================================

void BTreeNode::make_hints() {
  // Build hint array from evenly distributed key heads
  // hint[i] = key_head of entry at position dist * (i + 1)
  // where dist = count / (BTREE_HINT_COUNT + 1)
  if (entries.empty()) {
    hints.fill(0);
    return;
  }

  size_t count = entries.size();
  size_t dist = count / (BTREE_HINT_COUNT + 1);
  if (dist == 0)
    dist = 1;

  for (size_t i = 0; i < BTREE_HINT_COUNT; ++i) {
    size_t pos = dist * (i + 1);
    if (pos < count) {
      hints[i] = entries[pos].key_head;
    } else {
      hints[i] = 0;
    }
  }
}

void BTreeNode::update_hint(size_t slot_id) {
  // Efficiently update hints after a single insertion at slot_id
  // Only rebuild hints in the affected region
  size_t count = entries.size();
  if (count == 0)
    return;

  size_t dist = count / (BTREE_HINT_COUNT + 1);
  if (dist == 0)
    dist = 1;

  // Determine starting hint index to update
  // Based on TU-Munich: begin = max(0, (slotId / dist) - 1) if count > hintCount * 2 + 1
  size_t begin = 0;
  if (count > BTREE_HINT_COUNT * 2 + 1 && ((count - 1) / (BTREE_HINT_COUNT + 1)) == dist && (slot_id / dist) > 1) {
    begin = (slot_id / dist) - 1;
  }

  for (size_t i = begin; i < BTREE_HINT_COUNT; ++i) {
    size_t pos = dist * (i + 1);
    if (pos < count) {
      hints[i] = entries[pos].key_head;
    }
  }
}

void BTreeNode::search_hint(uint32_t key_head, size_t& lower_out, size_t& upper_out) const {
  // Use hint array to narrow the binary search range
  // Based on TU-Munich algorithm: searchHint
  if (BTREE_HINT_COUNT == 0 || entries.size() <= BTREE_HINT_COUNT * 2) {
    return;  // Hints not useful for small nodes
  }

  size_t dist = upper_out / (BTREE_HINT_COUNT + 1);
  if (dist == 0)
    return;

  // Find first hint >= key_head
  size_t pos = 0;
  for (; pos < BTREE_HINT_COUNT; ++pos) {
    if (hints[pos] >= key_head)
      break;
  }

  // Find last consecutive hint == key_head
  size_t pos2 = pos;
  for (; pos2 < BTREE_HINT_COUNT; ++pos2) {
    if (hints[pos2] != key_head)
      break;
  }

  // Narrow the range
  lower_out = pos * dist;
  if (pos2 < BTREE_HINT_COUNT) {
    upper_out = (pos2 + 1) * dist;
  }
}

size_t BTreeNode::lower_bound_optimized(const std::vector<AllTypeVariant>& key, bool& found_out) const {
  // Optimized lower bound search with full key comparison
  // Note: Unlike TU-Munich which uses order-preserving heads from raw bytes,
  // we use hash-based heads which don't preserve order. Therefore, we always
  // do full key comparisons but can still benefit from hint-based range narrowing.
  found_out = false;

  if (entries.empty()) {
    return 0;
  }

  // Compute key head for hint-based range narrowing
  uint32_t key_head = BTreeEntry::compute_head(key);

  // Use hints to narrow search range (even with hash-based heads,
  // this can help when there are many entries with different heads)
  size_t lower = 0;
  size_t upper = entries.size();
  search_hint(key_head, lower, upper);

  // Standard binary search with full key comparison
  while (lower < upper) {
    size_t mid = lower + (upper - lower) / 2;

    if (key < entries[mid].key) {
      upper = mid;
    } else if (key > entries[mid].key) {
      lower = mid + 1;
    } else {
      // Exact match
      found_out = true;
      return mid;
    }
  }

  return lower;
}

BTreeNode::SeparatorInfo BTreeNode::find_separator() const {
  // TU-Munich separator finding algorithm
  // Finds an optimal split point considering key distribution
  if (entries.size() < 2) {
    return {0, false};
  }

  size_t count = entries.size();

  if (!leaf) {
    // Inner nodes: split in the middle
    return {count / 2 - 1, false};
  }

  // For leaf nodes: find separator that minimizes prefix overlap
  // Based on TU-Munich algorithm
  size_t lower = count / 2 - count / 32;
  size_t upper = lower + count / 16;
  if (upper >= count)
    upper = count - 1;
  if (lower >= count)
    lower = count - 1;

  // Find common prefix between boundary entries
  size_t range_common_prefix = common_prefix(lower, upper);

  // Find first entry where the character at range_common_prefix differs
  for (size_t i = lower + 1; i <= upper; ++i) {
    if (i < count && common_prefix(lower, i) == range_common_prefix) {
      continue;
    }
    // Found a good split point
    return {i - 1, true};
  }

  return {lower, false};
}

size_t BTreeNode::common_prefix(size_t slot_a, size_t slot_b) const {
  // Compute common prefix length between two key vectors
  if (slot_a >= entries.size() || slot_b >= entries.size()) {
    return 0;
  }

  const auto& key_a = entries[slot_a].key;
  const auto& key_b = entries[slot_b].key;

  size_t min_len = std::min(key_a.size(), key_b.size());
  size_t prefix_len = 0;

  for (size_t i = 0; i < min_len; ++i) {
    if (key_a[i] == key_b[i]) {
      prefix_len++;
    } else {
      break;
    }
  }

  return prefix_len;
}

}  // namespace hyrise
