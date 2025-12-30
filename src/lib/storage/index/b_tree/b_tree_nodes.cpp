#include "b_tree_nodes.hpp"

#include <algorithm>

namespace hyrise {

BTreeNode::BTreeNode(int t, bool leaf) : t(t), leaf(leaf) {
  entries.reserve(2 * t - 1);
  children.reserve(2 * t);
}

void BTreeNode::insert_non_full(const std::vector<AllTypeVariant>& key, std::shared_ptr<BTreeValue> value) {
  auto i = static_cast<int>(entries.size()) - 1;

  if (leaf) {
    while (i >= 0 && key < entries[i].key) {
      i--;
    }
    entries.insert(entries.begin() + i + 1, {key, value});
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
}

std::shared_ptr<BTreeValue> BTreeNode::search(const std::vector<AllTypeVariant>& key) const {
  size_t i = 0;
  while (i < entries.size() && key > entries[i].key) {
    i++;
  }
  if (i < entries.size() && key == entries[i].key) {
    return entries[i].value;
  }
  if (leaf) {
    return nullptr;
  }
  return children[i]->search(key);
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

}  // namespace hyrise
