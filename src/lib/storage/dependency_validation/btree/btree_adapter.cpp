#include "storage/dependency_validation/btree/btree_adapter.hpp"

#include <algorithm>
#include <limits>

namespace hyrise::dv_tree {

namespace {
static_assert(engine::HINT_COUNT == 16, "the selected paper configuration uses 16 hints");
}

BTreeCppAdapter::BTreeCppAdapter() : tree_(new engine::BTree()) {}

BTreeCppAdapter::~BTreeCppAdapter() {
  delete tree_;
}

void* BTreeCppAdapter::find(std::string_view key) const {
  if (key.size() > std::numeric_limits<unsigned>::max())
    return nullptr;
  return tree_->lookup(reinterpret_cast<const uint8_t*>(key.data()), static_cast<unsigned>(key.size()));
}

void BTreeCppAdapter::insert_new(std::string_view key, void* payload) {
  if (key.size() > max_key_length()) {
    throw KeyTooLong("normalized key exceeds btree-cpp's structural page limit");
  }
  try {
    tree_->insert(reinterpret_cast<const uint8_t*>(key.data()), static_cast<unsigned>(key.size()), payload);
  } catch (const engine::DuplicateKeyError&) {
    throw DuplicateKey("insert_new called for an existing key");
  }
}

BTreeCppAdapter::Neighbors BTreeCppAdapter::neighbors(std::string_view key) const {
  const auto adjacent =
      tree_->neighbors(reinterpret_cast<const uint8_t*>(key.data()), static_cast<unsigned>(key.size()));
  return {adjacent.predecessor, adjacent.successor};
}

void BTreeCppAdapter::for_each(const std::function<bool(std::string_view, void*)>& visitor) const {
  tree_->for_each([&](const uint8_t* restored_key, unsigned restored_length, void* payload) {
    return visitor(std::string_view(reinterpret_cast<const char*>(restored_key), restored_length), payload);
  });
}

void BTreeCppAdapter::collect_stats(engine::Node* node, std::size_t depth, Stats& out) {
  ++out.nodes;
  out.height = std::max(out.height, depth);
  if (node->is_leaf()) {
    ++out.leaf_nodes;
    out.entries += node->count();
    out.max_prefix_length = std::max(out.max_prefix_length, static_cast<std::size_t>(node->prefix_length()));
    return;
  }
  ++out.inner_nodes;
  out.max_prefix_length = std::max(out.max_prefix_length, static_cast<std::size_t>(node->prefix_length()));
  for (unsigned i = 0; i < node->count(); ++i) {
    collect_stats(node->child(i), depth + 1, out);
  }
  collect_stats(node->upper(), depth + 1, out);
}

BTreeCppAdapter::Stats BTreeCppAdapter::stats() const {
  Stats result;
  collect_stats(tree_->root(), 1, result);
  engine::Node* previous = nullptr;
  for (engine::Node* leaf = tree_->first_leaf(); leaf != nullptr;
       leaf = leaf->control.next_leaf.load(std::memory_order_relaxed)) {
    ++result.linked_leaf_nodes;
    if (leaf->control.previous_leaf.load(std::memory_order_relaxed) != previous) {
      result.leaf_chain_valid = false;
    }
    previous = leaf;
  }
  if (result.linked_leaf_nodes != result.leaf_nodes)
    result.leaf_chain_valid = false;
  return result;
}

BTreeCppAdapter::MemoryStats BTreeCppAdapter::memory_stats() const {
  MemoryStats result;
  result.nodes = tree_->node_count();
  result.inner_nodes = tree_->inner_node_count();
  result.leaf_nodes = tree_->leaf_node_count();
  result.node_bytes = result.nodes * sizeof(engine::Node);
  result.published_image_bytes = result.nodes * sizeof(engine::PageBody);
  return result;
}

}  // namespace hyrise::dv_tree
