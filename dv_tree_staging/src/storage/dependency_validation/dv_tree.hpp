#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/dependency_validation/dv_types.hpp"

namespace hyrise::dv_tree {

struct DependencyEntry;
struct RhsRef;
class VersionedViolationHistory;
class DependencyValidator;
class DVTreeAccess;

// DV-Tree is the dependency-validation index. Its public surface deliberately
// hides the adapted TUM B+ tree, metadata latches, history container, and commit
// envelopes behind Impl.
class DVTree {
 public:
  // Portable logical accounting. Container/allocator overhead that cannot be
  // measured portably is intentionally excluded from the byte totals.
  struct MemoryStatistics {
    std::size_t tree_object_bytes = 0;

    std::size_t btree_inner_pages = 0;
    std::size_t btree_leaf_pages = 0;
    std::size_t btree_node_bytes = 0;
    std::size_t btree_published_image_bytes = 0;

    std::size_t dependency_entries = 0;
    std::size_t dependency_entry_object_bytes = 0;
    std::size_t dependency_owner_array_bytes = 0;
    std::size_t normalized_lhs_bytes = 0;
    std::size_t distinct_rhs_values = 0;
    uint64_t row_multiplicity = 0;
    std::size_t rhs_map_value_bytes = 0;

    std::size_t rhs_allocations = 0;
    std::size_t rhs_allocated_bytes = 0;
    std::size_t rhs_owner_array_bytes = 0;
    std::size_t rhs_live_entry_bytes = 0;

    std::size_t retained_history_entries = 0;
    std::size_t history_soft_capacity = 0;
    std::size_t history_value_bytes = 0;
    bool has_lowest_active_snapshot = false;
    CommitID lowest_active_snapshot = UNSET_COMMIT_ID;
    bool has_evicted_history = false;
    CommitID oldest_exact_snapshot = UNSET_COMMIT_ID;

    std::size_t active_commit_envelopes = 0;
    std::size_t staged_operations = 0;
    std::size_t footprint_keys = 0;
    std::size_t commit_state_bytes = 0;
    std::size_t prepared_entries = 0;
    std::size_t prepared_rhs_values = 0;
    std::size_t prepared_current_bytes = 0;
    std::size_t prepared_peak_bytes = 0;

    std::size_t total_accounted_bytes = 0;
  };

  struct EntrySnapshot {
    std::string lhs;
    std::vector<std::pair<std::string, uint64_t>> rhs_counts;
    uint64_t local_violations = 0;
    uint64_t neighbor_violation = 0;
    CommitID version = UNSET_COMMIT_ID;
  };

  class CommitTicket {
   public:
    CommitTicket();
    ~CommitTicket();
    CommitTicket(const CommitTicket&) = delete;
    CommitTicket& operator=(const CommitTicket&) = delete;
    CommitTicket(CommitTicket&& other) noexcept;
    CommitTicket& operator=(CommitTicket&& other) noexcept;

    CommitID commit_id() const;
    void stage(Transaction fragment);
    void insert(std::string lhs_norm, std::string rhs_norm);
    void remove(std::string lhs_norm, std::string rhs_norm);
    void update(std::string lhs_norm, std::string old_rhs_norm, std::string new_rhs_norm);
    void seal();
    void abort();
    // Wait for DV effects/history installation. Hyrise calls this before it
    // publishes the corresponding database commit.
    void wait_until_applied() const;
    // Additionally wait until advance_visibility_frontier() exposes the CID.
    void wait_until_visible() const;

   private:
    struct Impl;
    explicit CommitTicket(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
    friend class DVTree;
  };

  explicit DVTree(DependencyKind kind, std::size_t history_soft_capacity = 512,
                  CommitID initial_visible_cid = UNSET_COMMIT_ID);
  ~DVTree();
  DVTree(const DVTree&) = delete;
  DVTree& operator=(const DVTree&) = delete;

  MemoryStatistics memory_statistics() const;

  friend class DependencyValidator;
  friend class DVTreeAccess;

  // Maintenance and verdict surface. Production code reaches these methods only
  // through DVTreeAccess or the snapshot-checking DependencyValidator, so they
  // are private by default; DV_TESTING opens them for standalone white-box tests.
#ifndef DV_TESTING
 private:
#endif
  CommitTicket begin_commit();
  CommitTicket begin_commit(CommitID commit_id);
  void advance_registration_frontier(CommitID commit_id);
  void advance_visibility_frontier(CommitID commit_id);
  CommitID apply_commit(const Transaction& transaction);
  bool holds() const;
  int64_t violations() const;
  bool holds_at(CommitID snapshot) const;
  int64_t violation_count_at(CommitID snapshot) const;
  bool holds_exact(CommitID snapshot) const;
  int64_t violations_exact(CommitID snapshot) const;
  CommitID visible_commit_id() const;
  void set_lowest_active_snapshot(CommitID cid);
  void clear_lowest_active_snapshot();
  std::optional<EntrySnapshot> snapshot_entry(std::string_view lhs_norm) const;

#ifdef DV_TESTING
  // White-box-only inspection. These have no production counterpart.
  uint64_t max_concurrent_od_transactions_for_test() const;
  uint64_t optimistic_reprepare_count_for_test() const;
  uint64_t reservation_wait_count_for_test() const;
  std::size_t prepared_key_count_for_test(CommitID cid) const;
  bool effects_installed_for_test(CommitID cid) const;
  const VersionedViolationHistory& history() const;
  int64_t violations_live_uncommitted() const;
  DependencyEntry* find(std::string_view lhs_norm) const;
  DependencyEntry* first_entry() const;
  static std::string_view rhs_bytes(RhsRef ref);
  void compute_verdict();
#endif

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace hyrise::dv_tree
