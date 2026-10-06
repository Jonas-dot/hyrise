#include "public_api_smoke.hpp"

// Deliberately include only the standalone public headers. This translation
// unit guards against accidental public dependencies on src/ internals.
#include "storage/dependency_validation/dv_tree.hpp"
#include "storage/dependency_validation/normalized_key.hpp"

using namespace hyrise::dv_tree;  // NOLINT(build/namespaces)

bool public_api_smoke_test() {
  DVTree index(DependencyKind::FD, 8);
  Transaction transaction;
  transaction.insert(make_normalized_key("public-api"), make_normalized_key(1));
  const auto cid = index.apply_commit(transaction);
  const auto memory = index.memory_statistics();
  return cid == 1 && index.visible_commit_id() == cid && index.holds_exact(cid) && memory.btree_leaf_pages >= 1 &&
         memory.dependency_entries == 1 && memory.total_accounted_bytes > 0;
}
