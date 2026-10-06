#include <type_traits>

#include "storage/dependency_validation/dv_tree.hpp"
#include "types.hpp"

// The core's integral history representation remains distinct from Hyrise's
// public strong CommitID.
static_assert(!std::is_same_v<hyrise::dv_tree::InternalCommitID, hyrise::CommitID>);
