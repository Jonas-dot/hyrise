#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "storage/dependency_validation/btree/tum_btree/btree.hpp"

namespace hyrise::dv_tree {

// A deliberately thin boundary around the owned, adapted btree-cpp basic engine.
// The engine sees only arbitrary byte strings and one fixed-size pointer
// payload. Schema types, normalization, and dependency metadata stay above it.
class BTreeCppAdapter {
 public:
  struct Neighbors {
    void* predecessor = nullptr;
    void* successor = nullptr;
  };

  struct Stats {
    std::size_t nodes = 0;
    std::size_t inner_nodes = 0;
    std::size_t leaf_nodes = 0;
    std::size_t entries = 0;
    std::size_t height = 0;
    std::size_t max_prefix_length = 0;
    std::size_t linked_leaf_nodes = 0;
    bool leaf_chain_valid = true;
  };

  struct MemoryStats {
    std::size_t nodes = 0;
    std::size_t inner_nodes = 0;
    std::size_t leaf_nodes = 0;
    std::size_t node_bytes = 0;
    std::size_t published_image_bytes = 0;
  };

  struct KeyTooLong : std::length_error {
    using std::length_error::length_error;
  };

  struct DuplicateKey : std::logic_error {
    using std::logic_error::logic_error;
  };

  BTreeCppAdapter();
  ~BTreeCppAdapter();
  BTreeCppAdapter(const BTreeCppAdapter&) = delete;
  BTreeCppAdapter& operator=(const BTreeCppAdapter&) = delete;

  static constexpr std::size_t max_key_length() {
    return engine::Node::MAX_KEY_LENGTH;
  }

  void* find(std::string_view key) const;
  void insert_new(std::string_view key, void* payload);
  Neighbors neighbors(std::string_view absent_key) const;
  void for_each(const std::function<bool(std::string_view, void*)>& visitor) const;
  Stats stats() const;
  MemoryStats memory_stats() const;

#ifdef DV_TESTING
  using RestartPoint = engine::BTree::RestartPoint;

  void force_restart_once(RestartPoint point) {
    tree_->force_restart_once(point);
  }

  uint64_t restart_count(RestartPoint point) const {
    return tree_->restart_count(point);
  }
#endif

 private:
  static void collect_stats(engine::Node* node, std::size_t depth, Stats& out);

  engine::BTree* tree_;
};

}  // namespace hyrise::dv_tree
