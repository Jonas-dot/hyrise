#pragma once

// White-box regression suites intentionally inspect private engine and metadata
// state. Production users should include only dv_tree.hpp and normalized_key.hpp.
#include "storage/dependency_validation/btree/btree_adapter.hpp"
#include "storage/dependency_validation/btree/tum_btree/btree.hpp"
#include "storage/dependency_validation/dependency_entry.hpp"
#include "storage/dependency_validation/dv_tree.hpp"
#include "storage/dependency_validation/normalized_key.hpp"
#include "storage/dependency_validation/rhs_storage.hpp"
#include "storage/dependency_validation/versioned_history.hpp"

// The tests predate the move into the hyrise namespaces; bridge the old names
// so the suites read the same as when the index was fully standalone.
using namespace hyrise::dv_tree;
using hyrise::CommitID;
using hyrise::UNSET_COMMIT_ID;

namespace dv {
namespace engine = hyrise::dv_tree::engine;
}  // namespace dv

// ByteStringLess comes from rhs_storage.hpp via the using-directive above. The
// suites must not define their own: with dv_tree_impl.hpp in scope that would
// make every unqualified use ambiguous.
