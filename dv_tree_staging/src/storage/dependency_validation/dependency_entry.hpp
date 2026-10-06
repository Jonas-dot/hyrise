#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "storage/dependency_validation/rhs_storage.hpp"
#include "storage/dependency_validation/dv_types.hpp"

namespace hyrise::dv_tree {

// Stable, out-of-line metadata stored behind the B+ tree's fixed pointer payload.
// Entry addresses never change, so the sorted structural chain and mutex remain
// valid across page compaction and splits. This is deliberately a private type;
// white-box tests include it explicitly, while production users consume snapshots.
struct DependencyEntry {
  explicit DependencyEntry(std::string normalized_lhs) : lhs(std::move(normalized_lhs)) {}

  std::string lhs;
  std::map<RhsRef, uint64_t, RhsRefLess> rhs_counts;
  // Successor-only chain: FD and OD terms are both defined over the successor
  // direction, so no backward link is maintained.
  DependencyEntry* right = nullptr;
  uint64_t local_violations = 0;
  uint64_t neighbor_violation = 0;
  InternalCommitID version = INVALID_INTERNAL_COMMIT_ID;
  std::atomic<bool> nonempty{false};
  mutable std::mutex latch;

  std::size_t distinct_rhs() const {
    return rhs_counts.size();
  }

  RhsRef rhs_min() const {
    return rhs_counts.begin()->first;
  }

  RhsRef rhs_max() const {
    return rhs_counts.rbegin()->first;
  }
};

}  // namespace hyrise::dv_tree
