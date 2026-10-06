#pragma once

// White-box regression suites intentionally inspect private engine and metadata
// state. Production users should include only headers from include/dv/.
#include "storage/dependency_validation/btree/btree_adapter.hpp"
#include "storage/dependency_validation/btree/tum_btree/btree.hpp"
#include "storage/dependency_validation/dependency_entry.hpp"
#include "storage/dependency_validation/dv_tree.hpp"
#include "storage/dependency_validation/normalized_key.hpp"
#include "storage/dependency_validation/versioned_history.hpp"

struct ByteStringLess {
  bool operator()(std::string_view left, std::string_view right) const {
    return hyrise::dv_tree::compare_rhs_bytes(reinterpret_cast<const uint8_t*>(left.data()), left.size(),
                                              reinterpret_cast<const uint8_t*>(right.data()), right.size()) < 0;
  }
};
