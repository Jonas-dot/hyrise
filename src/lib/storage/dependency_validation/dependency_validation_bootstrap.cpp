#include "storage/dependency_validation/dependency_validation_bootstrap.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/dependency_validation/dv_tree.hpp"
#include "storage/dependency_validation/dv_tree_access.hpp"
#include "storage/dependency_validation/hyrise_value_normalization.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise::dv_tree {

namespace {

// The normalized keys are binary strings. Compare their bytes as unsigned
// values, then use length as the tie breaker. This deliberately does not use
// DVTree's comparison helpers, keeping the bootstrap oracle independent.
struct NormalizedKeyLess {
  bool operator()(const std::string& left, const std::string& right) const {
    const auto shared_length = std::min(left.size(), right.size());
    const auto comparison = shared_length == 0 ? 0 : std::memcmp(left.data(), right.data(), shared_length);
    return comparison == 0 ? left.size() < right.size() : comparison < 0;
  }
};

// Gathers one row's values for the given columns and normalizes them into a
// single self-delimiting composite key using the Phase 4 adapter.
std::string normalized_key_for_row(const std::vector<std::shared_ptr<AbstractSegment>>& segments,
                                   const std::vector<DataType>& column_types, const ChunkOffset offset) {
  auto values = std::vector<AllTypeVariant>{};
  values.reserve(segments.size());
  for (const auto& segment : segments) {
    values.push_back((*segment)[offset]);
  }
  return normalize_dependency_key(values, column_types);
}

}  // namespace

std::vector<NormalizedDependencyRow> scan_visible_dependency_rows(const Table& table, const DependencySpec& spec,
                                                                  const CommitID build_cid) {
  Assert(!spec.lhs_column_ids.empty(), "A dependency needs at least one LHS column.");
  Assert(!spec.rhs_column_ids.empty(), "A dependency needs at least one RHS column.");
  Assert(spec.lhs_column_ids.size() == spec.lhs_column_types.size(), "LHS column and type counts must match.");
  Assert(spec.rhs_column_ids.size() == spec.rhs_column_types.size(), "RHS column and type counts must match.");

  auto rows = std::vector<NormalizedDependencyRow>{};
  const auto chunk_count = table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }
    Assert(chunk->has_mvcc_data(), "Dependency bootstrap requires an MVCC-enabled table.");
    const auto mvcc_data = chunk->mvcc_data();

    // Resolve each dependency column's segment once per chunk.
    auto lhs_segments = std::vector<std::shared_ptr<AbstractSegment>>{};
    lhs_segments.reserve(spec.lhs_column_ids.size());
    for (const auto column_id : spec.lhs_column_ids) {
      lhs_segments.push_back(chunk->get_segment(column_id));
    }
    auto rhs_segments = std::vector<std::shared_ptr<AbstractSegment>>{};
    rhs_segments.reserve(spec.rhs_column_ids.size());
    for (const auto column_id : spec.rhs_column_ids) {
      rhs_segments.push_back(chunk->get_segment(column_id));
    }

    const auto chunk_size = chunk->size();
    for (auto offset = ChunkOffset{0}; offset < chunk_size; ++offset) {
      // Quiescent-table snapshot visibility: a committed row is part of the
      // build snapshot exactly when begin_cid <= build_cid < end_cid.
      const auto begin_cid = mvcc_data->get_begin_cid(offset);
      const auto end_cid = mvcc_data->get_end_cid(offset);
      if (!(begin_cid <= build_cid && build_cid < end_cid)) {
        continue;
      }

      auto lhs_norm = normalized_key_for_row(lhs_segments, spec.lhs_column_types, offset);
      auto rhs_norm = normalized_key_for_row(rhs_segments, spec.rhs_column_types, offset);
      rows.emplace_back(std::move(lhs_norm), std::move(rhs_norm));
    }
  }
  return rows;
}

int64_t batch_fd_violation_count(const std::vector<NormalizedDependencyRow>& rows) {
  auto distinct_rhs_by_lhs = std::unordered_map<std::string, std::unordered_set<std::string>>{};
  for (const auto& [lhs_norm, rhs_norm] : rows) {
    distinct_rhs_by_lhs[lhs_norm].insert(rhs_norm);
  }

  // Each LHS group contributes (distinct RHS values - 1) violations, matching
  // the DVTree's per-entry local_of() rule. Duplicate identical rows collapse
  // in the set and therefore add no violation.
  auto total = int64_t{0};
  for (const auto& [lhs_norm, distinct_rhs] : distinct_rhs_by_lhs) {
    total += static_cast<int64_t>(distinct_rhs.size()) - 1;
  }
  return total;
}

int64_t batch_od_violation_count(const std::vector<NormalizedDependencyRow>& rows) {
  // The outer map establishes the normalized LHS order. Each inner set
  // collapses duplicate rows and exposes the group's minimum/maximum RHS.
  auto rhs_by_lhs = std::map<std::string, std::set<std::string, NormalizedKeyLess>, NormalizedKeyLess>{};
  for (const auto& [lhs_norm, rhs_norm] : rows) {
    rhs_by_lhs[lhs_norm].insert(rhs_norm);
  }

  auto total = int64_t{0};
  const std::set<std::string, NormalizedKeyLess>* predecessor_rhs = nullptr;
  for (const auto& [lhs_norm, rhs_values] : rhs_by_lhs) {
    // OD retains the per-group ambiguity count used by FD: more than one RHS
    // for one LHS contributes distinct_rhs - 1 local violations.
    total += static_cast<int64_t>(rhs_values.size()) - 1;

    // A neighbor violation belongs to the predecessor group. It exists exactly
    // when its largest RHS is ordered after this group's smallest RHS.
    if (predecessor_rhs != nullptr && NormalizedKeyLess{}(*rhs_values.begin(), *predecessor_rhs->rbegin())) {
      ++total;
    }
    predecessor_rhs = &rhs_values;
  }
  return total;
}

std::shared_ptr<DVTree> build_dependency_validator(const Table& table, const DependencySpec& spec,
                                                   const CommitID build_cid) {
  Assert(build_cid >= INITIAL_COMMIT_ID, "The build snapshot CID must be a real commit ID.");
  Assert(build_cid < MAX_COMMIT_ID, "The build snapshot CID must be below the reserved MAX_COMMIT_ID.");

  const auto rows = scan_visible_dependency_rows(table, spec, build_cid);

  // Match the DVTree default soft history capacity; only the initial visible CID
  // needs overriding here.
  constexpr auto HISTORY_SOFT_CAPACITY = std::size_t{512};

  if (rows.empty()) {
    // No rows to install: an empty tree whose visible snapshot already sits at
    // build_cid answers holds_at(build_cid) == true without any commit.
    return std::make_shared<DVTree>(spec.kind, HISTORY_SOFT_CAPACITY, build_cid);
  }

  // Seed the tree one CID below the build snapshot so that its first (and only
  // bootstrap) local commit is reserved exactly at build_cid. The full dataset
  // is then installed as a single atomic state visible at build_cid.
  const auto baseline_cid = CommitID{static_cast<CommitID::base_type>(build_cid) - 1};
  auto tree = std::make_shared<DVTree>(spec.kind, HISTORY_SOFT_CAPACITY, baseline_cid);

  auto batch = Transaction{};
  for (const auto& [lhs_norm, rhs_norm] : rows) {
    batch.insert(lhs_norm, rhs_norm);
  }

  // Bootstrap is the first entry in a Hyrise-owned tree, so it must use the
  // external-CID ticket protocol as well. apply_commit() allocates an internal
  // CID and would permanently prohibit later coordinator tickets from using
  // Hyrise CIDs (and can leave their visibility wait blocked).
  auto ticket = DVTreeAccess::begin_commit(*tree, build_cid);
  ticket.stage(std::move(batch));
  ticket.seal();
  DVTreeAccess::advance_registration_frontier(*tree, build_cid);
  ticket.wait_until_applied();
  DVTreeAccess::advance_visibility_frontier(*tree, build_cid);
  ticket.wait_until_visible();
  Assert(DVTreeAccess::visible_commit_id(*tree) == build_cid,
         "The bootstrap commit did not land on the build snapshot CID.");

  const auto expected_violations =
      spec.kind == DependencyKind::FD ? batch_fd_violation_count(rows) : batch_od_violation_count(rows);
  Assert(DVTreeAccess::violation_count_at(*tree, build_cid) == expected_violations,
         "The built dependency validator disagrees with the independent batch oracle.");
  return tree;
}

}  // namespace hyrise::dv_tree
