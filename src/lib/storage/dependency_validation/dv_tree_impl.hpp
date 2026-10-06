#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/dependency_validation/btree/btree_adapter.hpp"
#include "storage/dependency_validation/dependency_entry.hpp"
#include "storage/dependency_validation/dv_tree.hpp"
#include "storage/dependency_validation/rhs_storage.hpp"
#include "storage/dependency_validation/versioned_history.hpp"

namespace hyrise::dv_tree {

class DVTreeImpl {
  struct CommitEnvelope;

 public:
  struct EntrySnapshot {
    std::string lhs;
    std::vector<std::pair<std::string, uint64_t>> rhs_counts;
    uint64_t local_violations = 0;
    uint64_t neighbor_violation = 0;
    InternalCommitID version = INVALID_INTERNAL_COMMIT_ID;
  };

  // Standalone model of Hyrise's CommitContext lifecycle. A ticket reserves
  // its CID immediately. The external-CID path additionally waits for a
  // registration frontier proving that no previously unseen lower CID can
  // still register with this tree. Once eligible, complete footprints are
  // registered in CID order, overlapping lower-CID reservations are awaited
  // without holding page/entry latches, and disjoint entry/OD-interval batches
  // may install out of order. History is drained in the order of CIDs actually
  // registered with this tree; gaps in Hyrise's global CID sequence are valid.
  class CommitTicket {
   public:
    CommitTicket() = default;
    CommitTicket(const CommitTicket&) = delete;
    CommitTicket& operator=(const CommitTicket&) = delete;

    CommitTicket(CommitTicket&& other) noexcept
        : owner_(other.owner_),
          envelope_(std::move(other.envelope_)),
          transaction_(std::move(other.transaction_)),
          resolved_(other.resolved_) {
      other.owner_ = nullptr;
      other.resolved_ = true;
    }

    CommitTicket& operator=(CommitTicket&& other) noexcept {
      if (this == &other)
        return *this;
      abandon_unresolved();
      owner_ = other.owner_;
      envelope_ = std::move(other.envelope_);
      transaction_ = std::move(other.transaction_);
      resolved_ = other.resolved_;
      other.owner_ = nullptr;
      other.resolved_ = true;
      return *this;
    }

    ~CommitTicket() {
      abandon_unresolved();
    }

    InternalCommitID commit_id() const;
    void stage(Transaction fragment);

    void insert(std::string lhs_norm, std::string rhs_norm) {
      ensure_staging();
      transaction_.insert(std::move(lhs_norm), std::move(rhs_norm));
    }

    void remove(std::string lhs_norm, std::string rhs_norm) {
      ensure_staging();
      transaction_.remove(std::move(lhs_norm), std::move(rhs_norm));
    }

    void update(std::string lhs_norm, std::string old_rhs_norm, std::string new_rhs_norm) {
      ensure_staging();
      transaction_.update(std::move(lhs_norm), std::move(old_rhs_norm), std::move(new_rhs_norm));
    }

    void seal();
    void abort();
    void wait_until_applied() const;
    void wait_until_visible() const;

   private:
    friend class DVTreeImpl;

    CommitTicket(DVTreeImpl* owner, std::shared_ptr<CommitEnvelope> envelope)
        : owner_(owner), envelope_(std::move(envelope)) {}

    void ensure_staging() const;
    void abandon_unresolved() noexcept;

    DVTreeImpl* owner_ = nullptr;
    std::shared_ptr<CommitEnvelope> envelope_;
    Transaction transaction_;
    bool resolved_ = false;
  };

  explicit DVTreeImpl(DependencyKind kind, std::size_t history_soft_capacity = 512,
                      InternalCommitID initial_visible_cid = INVALID_INTERNAL_COMMIT_ID)
      : kind_(kind),
        history_(history_soft_capacity),
        completed_commit_cid_(initial_visible_cid),
        visible_commit_cid_(initial_visible_cid),
        visibility_frontier_(initial_visible_cid),
        next_registered_cid_(initial_visible_cid + 1),
        registration_frontier_(initial_visible_cid) {}

  DVTreeImpl(const DVTreeImpl&) = delete;
  DVTreeImpl& operator=(const DVTreeImpl&) = delete;

  CommitTicket begin_commit();
  CommitTicket begin_commit(InternalCommitID commit_id);
  void advance_registration_frontier(InternalCommitID commit_id);
  void advance_visibility_frontier(InternalCommitID commit_id);

  // Convenience transaction API over the reservation protocol.
  InternalCommitID apply_commit(const Transaction& txn) {
    CommitTicket ticket = begin_commit();
    const InternalCommitID cid = ticket.commit_id();
    ticket.stage(txn);
    ticket.seal();
    try {
      ticket.wait_until_applied();
      ticket.wait_until_visible();
    } catch (...) {
      const std::exception_ptr error = std::current_exception();
      try {
        ticket.abort();
        ticket.wait_until_visible();
      } catch (...) {
        // Preserve the original validation/completion error. A failure
        // after effects were installed cannot be converted into a no-op.
      }
      std::rethrow_exception(error);
    }
    return cid;
  }

  bool holds() const {
    return violations() == 0;
  }

  int64_t violations() const {
    return history_.query(visible_commit_cid_.load(std::memory_order_acquire));
  }

  bool holds_exact(InternalCommitID snapshot) const {
    return violations_exact(snapshot) == 0;
  }

  int64_t violations_exact(InternalCommitID snapshot) const {
    if (snapshot > visible_commit_cid_.load(std::memory_order_acquire)) {
      throw SnapshotNotVisible("snapshot commit id is not visible yet");
    }
    return history_.query_exact(snapshot);
  }

  InternalCommitID visible_commit_id() const {
    return visible_commit_cid_.load(std::memory_order_acquire);
  }

#ifdef DV_TESTING
  uint64_t max_concurrent_od_transactions_for_test() const {
    return od_max_active_transactions_.load(std::memory_order_relaxed);
  }

  uint64_t optimistic_reprepare_count_for_test() const {
    return optimistic_reprepare_count_.load(std::memory_order_relaxed);
  }

  uint64_t reservation_wait_count_for_test() const {
    return reservation_wait_count_.load(std::memory_order_relaxed);
  }

  std::size_t prepared_key_count_for_test(InternalCommitID cid) const;
  bool effects_installed_for_test(InternalCommitID cid) const;
#endif

  void set_lowest_active_snapshot(InternalCommitID cid) {
    history_.set_lowest_active(cid);
  }

  void clear_lowest_active_snapshot() {
    history_.clear_lowest_active();
  }

  // Diagnostic/test access to retained-history metadata. Snapshot-aware clients
  // must use violations[_exact]()/holds[_exact]() so the watermark is enforced.
  const VersionedViolationHistory& history() const {
    return history_;
  }

  // Deliberately not committed-consistent. This supports the independent incremental
  // versus batch cross-check and may expose a transaction halfway through apply.
  int64_t violations_live_uncommitted() const {
    return live_violations_.load(std::memory_order_relaxed);
  }

  DependencyEntry* find(std::string_view lhs_norm) const {
    return find_unlocked(lhs_norm);
  }

  // Thread-safe latest-installed metadata inspection. It is intentionally not
  // an SI snapshot API: a disjoint higher CID may install before the global
  // completion prefix reaches it. Raw find()/first_entry() pointers remain
  // build/debug-only because their maps and cached counters are mutable.
  std::optional<EntrySnapshot> snapshot_entry(std::string_view lhs_norm) const {
    DependencyEntry* entry = find_unlocked(lhs_norm);
    if (entry == nullptr)
      return std::nullopt;
    const auto entry_guard = std::lock_guard<std::mutex>{entry->latch};
    return copy_entry_snapshot(*entry);
  }

  DVTree::MemoryStatistics memory_statistics() const {
    DVTree::MemoryStatistics result;
    result.tree_object_bytes = sizeof(DVTreeImpl);

    const auto btree = tree_.memory_stats();
    result.btree_inner_pages = btree.inner_nodes;
    result.btree_leaf_pages = btree.leaf_nodes;
    result.btree_node_bytes = btree.node_bytes;
    result.btree_published_image_bytes = btree.published_image_bytes;

    {
      const auto topology_guard = std::shared_lock<std::shared_mutex>{creation_mutex_};
      result.dependency_entries = entries_.size();
      result.dependency_entry_object_bytes = entries_.size() * sizeof(DependencyEntry);
      result.dependency_owner_array_bytes = entries_.capacity() * sizeof(decltype(entries_)::value_type);
      for (const auto& owned : entries_) {
        const DependencyEntry& entry = *owned;
        const auto entry_guard = std::lock_guard<std::mutex>{entry.latch};
        result.normalized_lhs_bytes += entry.lhs.size();
        result.distinct_rhs_values += entry.rhs_counts.size();
        result.rhs_map_value_bytes += entry.rhs_counts.size() * sizeof(decltype(entry.rhs_counts)::value_type);
        for (const auto& [rhs, count] : entry.rhs_counts) {
          result.rhs_live_entry_bytes += rhs.size;
          result.row_multiplicity += count;
        }
      }
    }

    {
      const auto rhs_guard = std::lock_guard<std::mutex>{rhs_storage_mutex_};
      const auto rhs = rhs_storage_.memory_stats();
      result.rhs_allocations = rhs.allocations;
      result.rhs_allocated_bytes = rhs.payload_bytes;
      result.rhs_owner_array_bytes = rhs.owner_array_bytes;
    }

    const auto history = history_.memory_stats();
    result.retained_history_entries = history.retained_entries;
    result.history_soft_capacity = history.soft_capacity;
    result.history_value_bytes = history.value_bytes;
    result.has_lowest_active_snapshot = history.has_lowest_active_snapshot;
    result.lowest_active_snapshot = to_hyrise_commit_id(history.lowest_active_snapshot);
    result.has_evicted_history = history.has_evicted_history;
    result.oldest_exact_snapshot = to_hyrise_commit_id(history.oldest_exact_snapshot);

    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      result.active_commit_envelopes = registered_commits_.size();
      for (const auto& [_, owned] : registered_commits_) {
        const CommitEnvelope& envelope = *owned;
        result.commit_state_bytes += sizeof(CommitEnvelope);
        result.staged_operations += envelope.transaction.ops.size();
        result.commit_state_bytes += envelope.transaction.ops.capacity() * sizeof(Transaction::Op);
        for (const auto& op : envelope.transaction.ops) {
          result.commit_state_bytes += op.lhs_norm.size() + op.rhs_norm.size();
        }
        result.footprint_keys += envelope.footprint.keys.size();
        result.commit_state_bytes += envelope.footprint.keys.capacity() * sizeof(std::string) +
                                     envelope.footprint.interval_low.size() + envelope.footprint.interval_high.size();
        for (const auto& key : envelope.footprint.keys) {
          result.commit_state_bytes += key.size();
        }
        result.commit_state_bytes +=
            envelope.fd_prepared_by_key.size() * sizeof(decltype(envelope.fd_prepared_by_key)::value_type);
        result.prepared_entries += envelope.prepared_entries;
        result.prepared_rhs_values += envelope.prepared_rhs_values;
        result.prepared_current_bytes += envelope.prepared_accounted_bytes;
      }
    }
    result.prepared_peak_bytes = prepared_bytes_peak_.load(std::memory_order_relaxed);

    result.total_accounted_bytes =
        result.tree_object_bytes + result.btree_node_bytes + result.btree_published_image_bytes +
        result.dependency_entry_object_bytes + result.dependency_owner_array_bytes + result.normalized_lhs_bytes +
        result.rhs_map_value_bytes + result.rhs_allocated_bytes + result.rhs_owner_array_bytes +
        result.history_value_bytes + result.commit_state_bytes + result.prepared_current_bytes;
    return result;
  }

  // Build/debug traversal contract: only use before writers start or after they join.
  DependencyEntry* first_entry() const {
    return first_;
  }

  static std::string_view rhs_bytes(RhsRef ref) {
    return {reinterpret_cast<const char*>(ref.data), ref.size};
  }

  // Independent O(n) reference recomputation. It updates only the live ledger and
  // entry caches, never the committed history.
  void compute_verdict() {
    const auto creation_guard = std::lock_guard<std::shared_mutex>{creation_mutex_};
    if (kind_ == DependencyKind::OD) {
      recompute_unlocked();
      return;
    }
    int64_t total = 0;
    for (DependencyEntry* entry = first_; entry != nullptr; entry = entry->right) {
      const auto entry_guard = std::lock_guard<std::mutex>{entry->latch};
      entry->local_violations = local_of(entry->distinct_rhs());
      entry->neighbor_violation = 0;
      total += static_cast<int64_t>(entry->local_violations);
    }
    live_violations_.store(total, std::memory_order_relaxed);
  }

 private:
  using CountMap = std::map<RhsRef, uint64_t, RhsRefLess>;

  struct OptimisticPreparedCommit;

  struct CommitFootprint {
    std::vector<std::string> keys;
    std::string interval_low;
    std::string interval_high;
    bool is_od = false;
    bool conservative_all = false;
  };

  struct CommitEnvelope {
    explicit CommitEnvelope(InternalCommitID commit_id) : cid(commit_id) {}

    const InternalCommitID cid;
    Transaction transaction;
    CommitFootprint footprint;
    std::map<std::string, std::shared_ptr<OptimisticPreparedCommit>, ByteStringLess> fd_prepared_by_key;
    std::exception_ptr completion_error;
    bool sealed = false;
    bool registration_started = false;
    bool footprint_registered = false;
    bool aborted = false;
    bool processing = false;
    bool effects_installed = false;
    bool completed = false;
    bool visible = false;
    int64_t delta = 0;
    std::size_t prepared_entries = 0;
    std::size_t prepared_rhs_values = 0;
    std::size_t prepared_accounted_bytes = 0;
  };

  enum class CommitIDSource { Unset, Internal, External };

#ifdef DV_TESTING
  class OdActiveGuard {
   public:
    OdActiveGuard(std::atomic<uint64_t>& active, std::atomic<uint64_t>& maximum) : active_(active) {
      const uint64_t now = active_.fetch_add(1, std::memory_order_acq_rel) + 1;
      uint64_t observed = maximum.load(std::memory_order_relaxed);
      while (observed < now && !maximum.compare_exchange_weak(observed, now, std::memory_order_relaxed)) {}
    }

    ~OdActiveGuard() {
      active_.fetch_sub(1, std::memory_order_acq_rel);
    }

   private:
    std::atomic<uint64_t>& active_;
  };
#endif

  struct PreparedEntry {
    DependencyEntry* entry = nullptr;
    CountMap rhs_counts;
    InternalCommitID observed_version = INVALID_INTERNAL_COMMIT_ID;
    uint64_t local_violations = 0;
    uint64_t neighbor_violation = 0;
    bool replace_rhs = false;
    bool touched = false;
    bool stamp_version = false;
  };

  struct OptimisticPreparedCommit {
    std::vector<PreparedEntry> entries;
    int64_t delta = 0;
    uint64_t topology_version = 0;
    bool is_od = false;
  };

  struct PreparedMemoryUsage {
    std::size_t entries = 0;
    std::size_t rhs_values = 0;
    std::size_t bytes = 0;
  };

  static PreparedMemoryUsage prepared_memory_usage(const OptimisticPreparedCommit& prepared) {
    PreparedMemoryUsage result;
    result.entries = prepared.entries.size();
    result.bytes = sizeof(OptimisticPreparedCommit) + prepared.entries.capacity() * sizeof(PreparedEntry);
    for (const auto& entry : prepared.entries) {
      result.rhs_values += entry.rhs_counts.size();
      result.bytes += entry.rhs_counts.size() * sizeof(CountMap::value_type);
    }
    return result;
  }

  void replace_prepared_accounting_locked(CommitEnvelope& envelope, PreparedMemoryUsage usage) {
    prepared_bytes_current_.fetch_sub(envelope.prepared_accounted_bytes, std::memory_order_relaxed);
    envelope.prepared_entries = usage.entries;
    envelope.prepared_rhs_values = usage.rhs_values;
    envelope.prepared_accounted_bytes = usage.bytes;
    const std::size_t current = prepared_bytes_current_.fetch_add(usage.bytes, std::memory_order_relaxed) + usage.bytes;
    std::size_t peak = prepared_bytes_peak_.load(std::memory_order_relaxed);
    while (peak < current && !prepared_bytes_peak_.compare_exchange_weak(peak, current, std::memory_order_relaxed)) {}
  }

  void clear_prepared_accounting_locked(CommitEnvelope& envelope) {
    prepared_bytes_current_.fetch_sub(envelope.prepared_accounted_bytes, std::memory_order_relaxed);
    envelope.prepared_entries = 0;
    envelope.prepared_rhs_values = 0;
    envelope.prepared_accounted_bytes = 0;
  }

  static uint64_t local_of(std::size_t distinct) {
    return distinct == 0 ? 0 : static_cast<uint64_t>(distinct - 1);
  }

  static EntrySnapshot copy_entry_snapshot(const DependencyEntry& entry) {
    EntrySnapshot result;
    result.lhs = entry.lhs;
    result.local_violations = entry.local_violations;
    result.neighbor_violation = entry.neighbor_violation;
    result.version = entry.version;
    result.rhs_counts.reserve(entry.rhs_counts.size());
    for (const auto& [rhs, count] : entry.rhs_counts) {
      result.rhs_counts.emplace_back(std::string(rhs_bytes(rhs)), count);
    }
    return result;
  }

  bool stage_add_rhs(CountMap& counts, std::string_view rhs_norm) {
    auto existing = counts.find(rhs_norm);
    if (existing != counts.end()) {
      ++existing->second;
      return false;
    }

    RhsRef ref;
    {
      const auto guard = std::lock_guard<std::mutex>{rhs_storage_mutex_};
      ref = rhs_storage_.store(rhs_norm);
    }
    counts.emplace(ref, 1);
    return true;
  }

  static bool stage_remove_rhs(CountMap& counts, std::string_view rhs_norm) {
    auto existing = counts.find(rhs_norm);
    if (existing == counts.end() || existing->second == 0) {
      throw MissingDependencyRow("delete of an RHS that is not present");
    }
    if (--existing->second == 0) {
      counts.erase(existing);
      return true;
    }
    return false;
  }

  static PreparedEntry& prepared_for(std::vector<PreparedEntry>& prepared, DependencyEntry* entry) {
    auto found = std::find_if(prepared.begin(), prepared.end(), [&](const PreparedEntry& item) {
      return item.entry == entry;
    });
    if (found == prepared.end()) {
      throw std::logic_error("transaction staging lost a resolved dependency entry");
    }
    return *found;
  }

  std::vector<DependencyEntry*> resolve_touched_entries(const Transaction& txn) {
    std::vector<DependencyEntry*> entries;
    entries.reserve(txn.ops.size());
    for (const auto& op : txn.ops) {
      DependencyEntry* entry = op.is_delete ? find_unlocked(op.lhs_norm) : get_or_create(op.lhs_norm);
      if (entry == nullptr)
        throw MissingDependencyRow("delete of an absent LHS");
      entries.push_back(entry);
    }
    std::sort(entries.begin(), entries.end(), [](const auto* left, const auto* right) {
      return ByteStringLess{}(left->lhs, right->lhs);
    });
    entries.erase(std::unique(entries.begin(), entries.end()), entries.end());
    return entries;
  }

  CommitFootprint build_commit_footprint(const Transaction& txn) {
    CommitFootprint footprint;
    footprint.is_od = kind_ == DependencyKind::OD;
    footprint.keys.reserve(txn.ops.size());
    for (const auto& op : txn.ops)
      footprint.keys.push_back(op.lhs_norm);
    std::sort(footprint.keys.begin(), footprint.keys.end(), ByteStringLess{});
    footprint.keys.erase(std::unique(footprint.keys.begin(), footprint.keys.end()), footprint.keys.end());
    if (!footprint.is_od || footprint.keys.empty())
      return footprint;

    // Structural tombstones are safe to create during footprint registration:
    // no RHS metadata becomes visible, and a later definitive delete error can
    // still abort the transaction. Creating every directly touched LHS lets a
    // higher CID conservatively locate its interval even when a lower CID has
    // not installed the entry's first row yet.
    std::vector<DependencyEntry*> touched;
    touched.reserve(footprint.keys.size());
    for (const auto& lhs : footprint.keys)
      touched.push_back(get_or_create(lhs));

    const auto topology_guard = std::shared_lock<std::shared_mutex>{creation_mutex_};
    std::vector<DependencyEntry*> chain;
    for (DependencyEntry* entry = first_; entry != nullptr; entry = entry->right) {
      chain.push_back(entry);
    }
    auto chain_index = [&](DependencyEntry* needle) {
      const auto found = std::find(chain.begin(), chain.end(), needle);
      if (found == chain.end()) {
        throw std::logic_error("OD footprint lost a dependency entry");
      }
      return static_cast<std::size_t>(found - chain.begin());
    };

    std::size_t begin = chain_index(touched.front());
    const std::size_t last_touched = chain_index(touched.back());
    bool predecessor_found = false;
    while (begin > 0) {
      --begin;
      if (chain[begin]->nonempty.load(std::memory_order_acquire)) {
        predecessor_found = true;
        break;
      }
    }
    if (!predecessor_found)
      begin = 0;

    std::size_t end = last_touched;
    bool successor_found = false;
    while (end + 1 < chain.size()) {
      ++end;
      if (chain[end]->nonempty.load(std::memory_order_acquire)) {
        successor_found = true;
        break;
      }
    }
    if (!successor_found)
      end = chain.size() - 1;

    footprint.interval_low = chain[begin]->lhs;
    footprint.interval_high = chain[end]->lhs;
    return footprint;
  }

  static bool footprints_overlap(const CommitFootprint& left, const CommitFootprint& right) {
    if (left.conservative_all || right.conservative_all)
      return true;
    if (left.keys.empty() || right.keys.empty())
      return false;
    if (left.is_od || right.is_od) {
      return !ByteStringLess{}(left.interval_high, right.interval_low) &&
             !ByteStringLess{}(right.interval_high, left.interval_low);
    }

    std::size_t left_index = 0;
    std::size_t right_index = 0;
    while (left_index < left.keys.size() && right_index < right.keys.size()) {
      if (left.keys[left_index] == right.keys[right_index])
        return true;
      if (ByteStringLess{}(left.keys[left_index], right.keys[right_index])) {
        ++left_index;
      } else {
        ++right_index;
      }
    }
    return false;
  }

  std::shared_ptr<OptimisticPreparedCommit> prepare_fd_optimistic(const Transaction& txn) {
    auto result = std::make_shared<OptimisticPreparedCommit>();
    result->is_od = false;
    std::vector<DependencyEntry*> touched = resolve_touched_entries(txn);
    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(touched.size());
    for (DependencyEntry* entry : touched)
      locks.emplace_back(entry->latch);

    result->entries.reserve(touched.size());
    for (DependencyEntry* entry : touched) {
      PreparedEntry item;
      item.entry = entry;
      item.rhs_counts = entry->rhs_counts;
      item.observed_version = entry->version;
      item.local_violations = entry->local_violations;
      item.neighbor_violation = 0;
      item.replace_rhs = true;
      item.touched = true;
      item.stamp_version = true;
      result->entries.push_back(std::move(item));
    }

    for (const auto& op : txn.ops) {
      PreparedEntry& item = prepared_for(result->entries, find_unlocked(op.lhs_norm));
      if (op.is_delete)
        stage_remove_rhs(item.rhs_counts, op.rhs_norm);
      else
        stage_add_rhs(item.rhs_counts, op.rhs_norm);
    }
    for (auto& item : result->entries) {
      item.local_violations = local_of(item.rhs_counts.size());
      result->delta += static_cast<int64_t>(item.local_violations) - static_cast<int64_t>(item.entry->local_violations);
    }
    return result;
  }

  std::shared_ptr<OptimisticPreparedCommit> prepare_od_optimistic(const Transaction& txn) {
    auto result = std::make_shared<OptimisticPreparedCommit>();
    result->is_od = true;
    const std::vector<DependencyEntry*> touched = resolve_touched_entries(txn);

    const auto topology_guard = std::shared_lock<std::shared_mutex>{creation_mutex_};
    result->topology_version = topology_version_.load(std::memory_order_acquire);
    if (touched.empty())
      return result;

    std::vector<DependencyEntry*> chain;
    for (DependencyEntry* entry = first_; entry != nullptr; entry = entry->right) {
      chain.push_back(entry);
    }
    auto chain_index = [&](DependencyEntry* needle) {
      const auto found = std::find(chain.begin(), chain.end(), needle);
      if (found == chain.end()) {
        throw std::logic_error("OD optimistic preparation lost a dependency entry");
      }
      return static_cast<std::size_t>(found - chain.begin());
    };
    const std::size_t first_touched = chain_index(touched.front());
    const std::size_t last_touched = chain_index(touched.back());

    auto affected_bounds = [&] {
      std::size_t begin = first_touched;
      bool predecessor_found = false;
      while (begin > 0) {
        --begin;
        if (chain[begin]->nonempty.load(std::memory_order_acquire)) {
          predecessor_found = true;
          break;
        }
      }
      if (!predecessor_found)
        begin = 0;

      std::size_t end = last_touched;
      bool successor_found = false;
      while (end + 1 < chain.size()) {
        ++end;
        if (chain[end]->nonempty.load(std::memory_order_acquire)) {
          successor_found = true;
          break;
        }
      }
      if (!successor_found)
        end = chain.size() - 1;
      return std::pair<std::size_t, std::size_t>{begin, end};
    };

    std::size_t begin = 0;
    std::size_t end = 0;
    std::vector<std::unique_lock<std::mutex>> locks;
    for (;;) {
      const auto proposed = affected_bounds();
      begin = proposed.first;
      end = proposed.second;
      locks.clear();
      locks.reserve(end - begin + 1);
      for (std::size_t i = begin; i <= end; ++i) {
        locks.emplace_back(chain[i]->latch);
      }
      const auto verified = affected_bounds();
      if (verified.first >= begin && verified.second <= end)
        break;
      locks.clear();
    }

    result->entries.reserve(end - begin + 1);
    for (std::size_t i = begin; i <= end; ++i) {
      DependencyEntry* entry = chain[i];
      PreparedEntry item;
      item.entry = entry;
      item.rhs_counts = entry->rhs_counts;
      item.observed_version = entry->version;
      item.local_violations = entry->local_violations;
      item.neighbor_violation = entry->neighbor_violation;
      item.touched = std::find(touched.begin(), touched.end(), entry) != touched.end();
      item.replace_rhs = item.touched;
      item.stamp_version = item.touched;
      result->entries.push_back(std::move(item));
    }

    for (const auto& op : txn.ops) {
      PreparedEntry& item = prepared_for(result->entries, find_unlocked(op.lhs_norm));
      if (op.is_delete)
        stage_remove_rhs(item.rhs_counts, op.rhs_norm);
      else
        stage_add_rhs(item.rhs_counts, op.rhs_norm);
    }
    for (auto& item : result->entries) {
      if (!item.touched)
        continue;
      const uint64_t updated_local = local_of(item.rhs_counts.size());
      result->delta += static_cast<int64_t>(updated_local) - static_cast<int64_t>(item.entry->local_violations);
      item.local_violations = updated_local;
    }

    const std::size_t affected_neighbor_count = last_touched - begin + 1;
    for (std::size_t i = 0; i < result->entries.size(); ++i) {
      if (i >= affected_neighbor_count)
        break;
      auto& item = result->entries[i];
      const uint64_t old_neighbor = item.entry->neighbor_violation;
      item.neighbor_violation = 0;
      if (!item.rhs_counts.empty()) {
        std::size_t successor = i + 1;
        while (successor < result->entries.size() && result->entries[successor].rhs_counts.empty()) {
          ++successor;
        }
        if (successor < result->entries.size()) {
          const RhsRef maximum = item.rhs_counts.rbegin()->first;
          const RhsRef minimum = result->entries[successor].rhs_counts.begin()->first;
          item.neighbor_violation =
              compare_rhs_bytes(maximum.data, maximum.size, minimum.data, minimum.size) > 0 ? 1 : 0;
        }
      }
      result->delta += static_cast<int64_t>(item.neighbor_violation) - static_cast<int64_t>(old_neighbor);
      if (item.neighbor_violation != old_neighbor)
        item.stamp_version = true;
    }
    return result;
  }

  std::shared_ptr<OptimisticPreparedCommit> prepare_optimistic(const Transaction& txn) {
    return kind_ == DependencyKind::OD ? prepare_od_optimistic(txn) : prepare_fd_optimistic(txn);
  }

  bool try_install_reserved(OptimisticPreparedCommit& prepared, InternalCommitID cid) {
    std::shared_lock<std::shared_mutex> topology_guard;
    if (prepared.is_od) {
      topology_guard = std::shared_lock<std::shared_mutex>(creation_mutex_);
      if (topology_version_.load(std::memory_order_acquire) != prepared.topology_version) {
        return false;
      }
    }

    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(prepared.entries.size());
    for (auto& item : prepared.entries)
      locks.emplace_back(item.entry->latch);
    for (const auto& item : prepared.entries) {
      if (item.entry->version != item.observed_version)
        return false;
    }

    // All allocations and validation happened during private preparation.
    // Reservations prevent any higher conflicting CID from installing until
    // this complete batch is installed and marked completed.
    for (auto& item : prepared.entries) {
      if (item.replace_rhs) {
        item.entry->rhs_counts.swap(item.rhs_counts);
        item.entry->nonempty.store(!item.entry->rhs_counts.empty(), std::memory_order_release);
      }
      item.entry->local_violations = item.local_violations;
      item.entry->neighbor_violation = item.neighbor_violation;
      if (item.stamp_version)
        item.entry->version = cid;
    }
    live_violations_.fetch_add(prepared.delta, std::memory_order_relaxed);
    return true;
  }

  void register_ready_footprints() {
    auto driver_guard = std::unique_lock<std::mutex>{registration_driver_mutex_};
    for (;;) {
      std::shared_ptr<CommitEnvelope> envelope;
      Transaction transaction;
      bool aborted = false;
      {
        const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
        const auto found = std::find_if(registered_commits_.begin(), registered_commits_.end(), [](const auto& item) {
          return !item.second->footprint_registered;
        });
        if (found == registered_commits_.end() || found->first > registration_frontier_)
          return;
        envelope = found->second;
        if (!envelope->sealed || envelope->registration_started || envelope->footprint_registered) {
          return;
        }
        envelope->registration_started = true;
        transaction = envelope->transaction;
        aborted = envelope->aborted;
      }

      CommitFootprint footprint;
      std::exception_ptr registration_error;
      if (!aborted) {
        try {
          footprint = build_commit_footprint(transaction);
        } catch (...) {
          registration_error = std::current_exception();
          footprint.conservative_all = true;
        }
      }

      {
        const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
        envelope->footprint = std::move(footprint);
        envelope->registration_started = false;
        envelope->footprint_registered = true;
        envelope->completion_error = registration_error;
        if (aborted)
          envelope->completed = true;
      }
      registered_cv_.notify_all();
    }
  }

  void process_ready_commits() {
    register_ready_footprints();

    std::vector<std::shared_ptr<CommitEnvelope>> ready;
    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      for (const auto& [cid, envelope] : registered_commits_) {
        if (cid > registration_frontier_)
          break;
        if (envelope->footprint_registered && !envelope->completed && !envelope->aborted &&
            !envelope->completion_error) {
          ready.push_back(envelope);
        }
      }
    }
    for (const auto& envelope : ready)
      execute_registered(envelope, false);
    advance_completed_prefix();
  }

  void refresh_visible_commit_id_locked() {
    const InternalCommitID completed = completed_commit_cid_.load(std::memory_order_relaxed);
    const InternalCommitID visible = std::min(completed, visibility_frontier_);
    if (visible > visible_commit_cid_.load(std::memory_order_relaxed))
      visible_commit_cid_.store(visible, std::memory_order_release);
  }

  bool has_lower_conflict_locked(const CommitEnvelope& envelope) const {
    for (auto current = registered_commits_.begin();
         current != registered_commits_.end() && current->first < envelope.cid; ++current) {
      const CommitEnvelope& lower = *current->second;
      if (lower.footprint_registered && !lower.completed && footprints_overlap(lower.footprint, envelope.footprint)) {
        return true;
      }
    }
    return false;
  }

  // Authoritative OD reservation check, tested against the window a transaction
  // actually prepared rather than the interval it registered when it sealed.
  //
  // A registered interval reaches from the nearest non-empty predecessor to the
  // nearest non-empty successor, so it describes the neighbour structure as it
  // was at seal time. A lower CID that tombstones the groups in between makes
  // previously separated groups neighbours, and the window recomputed at
  // preparation time is then wider than the registered interval. Ordering by the
  // registered interval alone therefore lets a higher CID install first on a
  // shared term, which keeps the installed state correct (versions and topology
  // are still validated) but measures the two deltas against the wrong states
  // for history's CID-ordered attribution.
  //
  // Testing the prepared window closes that gap. Every way a lower CID can
  // disturb a term inside this window -- changing a compared group's maximum,
  // changing the successor group's minimum, or tombstoning/resurrecting a group
  // between them -- runs through one of that CID's own touched keys, and a
  // footprint's key list is a pure function of its transaction, so it never goes
  // stale even when its interval does.
  bool has_lower_conflict_with_range_locked(const CommitEnvelope& envelope, std::string_view window_low,
                                            std::string_view window_high) const {
    const auto less = ByteStringLess{};
    for (auto current = registered_commits_.begin();
         current != registered_commits_.end() && current->first < envelope.cid; ++current) {
      const CommitEnvelope& lower = *current->second;
      if (!lower.footprint_registered || lower.completed)
        continue;
      if (lower.footprint.conservative_all)
        return true;
      if (lower.footprint.keys.empty())
        continue;
      // The interval is a superset of the touched keys in the state that built
      // it; fall back to the exact keys when no interval was recorded.
      const std::string_view lower_low =
          lower.footprint.interval_low.empty() ? lower.footprint.keys.front() : lower.footprint.interval_low;
      const std::string_view lower_high =
          lower.footprint.interval_high.empty() ? lower.footprint.keys.back() : lower.footprint.interval_high;
      if (!less(lower_high, window_low) && !less(window_high, lower_low))
        return true;
    }
    return false;
  }

  bool has_lower_key_conflict_locked(const CommitEnvelope& envelope, std::string_view key) const {
    for (auto current = registered_commits_.begin();
         current != registered_commits_.end() && current->first < envelope.cid; ++current) {
      const CommitEnvelope& lower = *current->second;
      if (!lower.footprint_registered || lower.completed)
        continue;
      if (lower.footprint.conservative_all ||
          std::binary_search(lower.footprint.keys.begin(), lower.footprint.keys.end(), key, ByteStringLess{})) {
        return true;
      }
    }
    return false;
  }

  void advance_completed_prefix() {
    auto completion_guard = std::unique_lock<std::mutex>{completion_driver_mutex_};
    for (;;) {
      std::shared_ptr<CommitEnvelope> envelope;
      InternalCommitID completed_cid = INVALID_INTERNAL_COMMIT_ID;
      int64_t delta = 0;
      {
        const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
        const auto found = registered_commits_.begin();
        if (found == registered_commits_.end() || !found->second->completed) {
          const InternalCommitID gap_end = found == registered_commits_.end()
                                               ? registration_frontier_
                                               : std::min(registration_frontier_, found->first - 1);
          if (gap_end > completed_commit_cid_.load(std::memory_order_relaxed)) {
            completed_commit_cid_.store(gap_end, std::memory_order_release);
            if (commit_id_source_ != CommitIDSource::External)
              visibility_frontier_ = gap_end;
            refresh_visible_commit_id_locked();
            registered_cv_.notify_all();
          }
          return;
        }
        envelope = found->second;
        completed_cid = found->first;
        delta = envelope->delta;
      }

      try {
        history_.update_reserved(completed_cid, delta);
      } catch (...) {
        {
          const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
          envelope->completion_error = std::current_exception();
        }
        registered_cv_.notify_all();
        return;
      }

      {
        const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
        envelope->visible = true;
        envelope->completion_error = nullptr;
        completed_commit_cid_.store(completed_cid, std::memory_order_release);
        if (commit_id_source_ != CommitIDSource::External)
          visibility_frontier_ = completed_cid;
        refresh_visible_commit_id_locked();
        registered_commits_.erase(completed_cid);
      }
      registered_cv_.notify_all();
    }
  }

  void execute_registered(const std::shared_ptr<CommitEnvelope>& envelope, bool wait_for_conflicts) {
    Transaction transaction;
#ifdef DV_TESTING
    bool counted_wait = false;
#endif
    for (;;) {
      auto registry_guard = std::unique_lock<std::mutex>{registered_mutex_};
      if (wait_for_conflicts) {
        registered_cv_.wait(registry_guard, [&] {
          return envelope->footprint_registered || envelope->visible || envelope->completion_error;
        });
      }
      if (!envelope->footprint_registered || envelope->visible || envelope->completed || envelope->aborted ||
          envelope->completion_error) {
        return;
      }
      if (envelope->processing) {
        if (!wait_for_conflicts)
          return;
        registered_cv_.wait(registry_guard, [&] {
          return !envelope->processing || envelope->visible || envelope->completion_error;
        });
        continue;
      }
      if (kind_ == DependencyKind::OD && has_lower_conflict_locked(*envelope)) {
        if (!wait_for_conflicts)
          return;
#ifdef DV_TESTING
        if (!counted_wait) {
          reservation_wait_count_.fetch_add(1, std::memory_order_relaxed);
          counted_wait = true;
        }
#endif
        registered_cv_.wait(registry_guard, [&] {
          return !has_lower_conflict_locked(*envelope) || envelope->completion_error || envelope->aborted;
        });
        continue;
      }
      envelope->processing = true;
      transaction = envelope->transaction;
      break;
    }

    std::shared_ptr<OptimisticPreparedCommit> prepared;
    std::exception_ptr execution_error;
    try {
#ifdef DV_TESTING
      std::unique_ptr<OdActiveGuard> od_active_guard;
      if (kind_ == DependencyKind::OD) {
        od_active_guard = std::make_unique<OdActiveGuard>(od_active_transactions_, od_max_active_transactions_);
      }
#endif
      if (kind_ == DependencyKind::FD) {
        for (;;) {
          std::vector<std::string> keys;
          {
            const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
            keys = envelope->footprint.keys;
          }

          bool made_progress = false;
          for (const auto& key : keys) {
            {
              const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
              if (envelope->fd_prepared_by_key.find(key) != envelope->fd_prepared_by_key.end()) {
                continue;
              }
              if (has_lower_key_conflict_locked(*envelope, key))
                continue;
            }

            Transaction fragment;
            for (const auto& op : transaction.ops) {
              if (op.lhs_norm == key)
                fragment.ops.push_back(op);
            }
            auto partial = prepare_fd_optimistic(fragment);
            const PreparedMemoryUsage partial_usage = prepared_memory_usage(*partial);
            {
              const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
              envelope->fd_prepared_by_key.emplace(key, std::move(partial));
              replace_prepared_accounting_locked(*envelope, {envelope->prepared_entries + partial_usage.entries,
                                                             envelope->prepared_rhs_values + partial_usage.rhs_values,
                                                             envelope->prepared_accounted_bytes + partial_usage.bytes});
            }
            made_progress = true;
          }

          auto registry_guard = std::unique_lock<std::mutex>{registered_mutex_};
          if (envelope->fd_prepared_by_key.size() == keys.size())
            break;
          if (!wait_for_conflicts) {
            envelope->processing = false;
            registry_guard.unlock();
            registered_cv_.notify_all();
            return;
          }
#ifdef DV_TESTING
          if (!counted_wait) {
            reservation_wait_count_.fetch_add(1, std::memory_order_relaxed);
            counted_wait = true;
          }
#endif
          if (!made_progress) {
            registered_cv_.wait(registry_guard, [&] {
              if (envelope->completion_error || envelope->aborted)
                return true;
              for (const auto& key : keys) {
                if (envelope->fd_prepared_by_key.find(key) == envelope->fd_prepared_by_key.end() &&
                    !has_lower_key_conflict_locked(*envelope, key)) {
                  return true;
                }
              }
              return false;
            });
          }
          if (envelope->completion_error || envelope->aborted) {
            throw CommitOrderViolation("reserved commit was cancelled while waiting");
          }
        }

        prepared = std::make_shared<OptimisticPreparedCommit>();
        prepared->is_od = false;
        std::vector<std::shared_ptr<OptimisticPreparedCommit>> partials;
        {
          const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
          for (const auto& [_, partial] : envelope->fd_prepared_by_key) {
            partials.push_back(partial);
          }
        }
        for (auto& partial : partials) {
          prepared->delta += partial->delta;
          prepared->entries.insert(prepared->entries.end(), std::make_move_iterator(partial->entries.begin()),
                                   std::make_move_iterator(partial->entries.end()));
        }
      } else {
        prepared = prepare_od_optimistic(transaction);
        const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
        replace_prepared_accounting_locked(*envelope, prepared_memory_usage(*prepared));
      }
      auto reprepare = [&] {
        auto replacement = prepare_optimistic(transaction);
        {
          const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
          replace_prepared_accounting_locked(*envelope, prepared_memory_usage(*replacement));
        }
        prepared = std::move(replacement);
      };

      for (;;) {
        // The window just prepared may be wider than the registered interval, so
        // re-establish the reservation against it before installing. Entry
        // addresses and their normalized LHS are stable for the tree's lifetime,
        // so these bounds stay valid across a reprepare.
        if (kind_ == DependencyKind::OD && !prepared->entries.empty()) {
          const std::string_view window_low = prepared->entries.front().entry->lhs;
          const std::string_view window_high = prepared->entries.back().entry->lhs;
          auto registry_guard = std::unique_lock<std::mutex>{registered_mutex_};
          if (has_lower_conflict_with_range_locked(*envelope, window_low, window_high)) {
            if (!wait_for_conflicts) {
              envelope->processing = false;
              clear_prepared_accounting_locked(*envelope);
              registry_guard.unlock();
              registered_cv_.notify_all();
              return;
            }
#ifdef DV_TESTING
            if (!counted_wait) {
              reservation_wait_count_.fetch_add(1, std::memory_order_relaxed);
              counted_wait = true;
            }
#endif
            registered_cv_.wait(registry_guard, [&] {
              return !has_lower_conflict_with_range_locked(*envelope, window_low, window_high) ||
                     envelope->completion_error || envelope->aborted;
            });
            if (envelope->completion_error || envelope->aborted) {
              throw CommitOrderViolation("reserved commit was cancelled while waiting");
            }
            registry_guard.unlock();
            // The lower CID installed while this transaction waited, so the
            // neighbour structure it observed is no longer the one it must
            // measure its delta against.
            reprepare();
            continue;
          }
        }

        if (try_install_reserved(*prepared, envelope->cid))
          break;
#ifdef DV_TESTING
        optimistic_reprepare_count_.fetch_add(1, std::memory_order_relaxed);
#endif
        reprepare();
      }
    } catch (...) {
      execution_error = std::current_exception();
    }

    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      envelope->processing = false;
      clear_prepared_accounting_locked(*envelope);
      if (execution_error) {
        envelope->completion_error = execution_error;
      } else {
        envelope->effects_installed = true;
        envelope->completed = true;
        envelope->delta = prepared->delta;
      }
    }
    registered_cv_.notify_all();
    if (!execution_error)
      advance_completed_prefix();
  }

  void seal_registered(const std::shared_ptr<CommitEnvelope>& envelope, Transaction transaction) {
    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      if (envelope->sealed || envelope->visible) {
        throw CommitOrderViolation("registered commit is already resolved");
      }
      envelope->transaction = std::move(transaction);
      envelope->sealed = true;
    }
    process_ready_commits();
  }

  void abort_registered(const std::shared_ptr<CommitEnvelope>& envelope) {
    bool already_visible = false;
    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      already_visible = envelope->visible;
      if (already_visible)
        return;
      if (envelope->processing || envelope->effects_installed) {
        throw CommitOrderViolation("cannot abort a commit after installation started");
      }
      envelope->transaction.ops.clear();
      clear_prepared_accounting_locked(*envelope);
      envelope->fd_prepared_by_key.clear();
      envelope->completion_error = nullptr;
      envelope->sealed = true;
      envelope->aborted = true;
      envelope->delta = 0;
      if (envelope->footprint_registered)
        envelope->completed = true;
    }
    process_ready_commits();
    {
      const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
      if (envelope->footprint_registered)
        envelope->completed = true;
    }
    registered_cv_.notify_all();
    advance_completed_prefix();
  }

  void wait_registered(const std::shared_ptr<CommitEnvelope>& envelope) {
    execute_registered(envelope, true);
    auto registry_guard = std::unique_lock<std::mutex>{registered_mutex_};
    registered_cv_.wait(registry_guard, [&] {
      return envelope->visible || envelope->completion_error;
    });
    if (envelope->completion_error) {
      std::rethrow_exception(envelope->completion_error);
    }
  }

  void wait_globally_visible(const std::shared_ptr<CommitEnvelope>& envelope) {
    wait_registered(envelope);
    auto registry_guard = std::unique_lock<std::mutex>{registered_mutex_};
    registered_cv_.wait(registry_guard, [&] {
      return visible_commit_cid_.load(std::memory_order_acquire) >= envelope->cid;
    });
  }

  DependencyEntry* find_unlocked(std::string_view lhs_norm) const {
    return static_cast<DependencyEntry*>(tree_.find(lhs_norm));
  }

  DependencyEntry* get_or_create(std::string_view lhs_norm) {
    if (DependencyEntry* existing = find_unlocked(lhs_norm))
      return existing;

    // The B+ tree itself is OLC. This topology latch only serializes ownership
    // allocation and the separate DependencyEntry neighbor-chain splice.
    const auto guard = std::lock_guard<std::shared_mutex>{creation_mutex_};
    if (DependencyEntry* existing = find_unlocked(lhs_norm))
      return existing;

    const BTreeCppAdapter::Neighbors neighbors = tree_.neighbors(lhs_norm);
    auto owned = std::make_unique<DependencyEntry>(std::string(lhs_norm));
    DependencyEntry* entry = owned.get();
    DependencyEntry* predecessor = static_cast<DependencyEntry*>(neighbors.predecessor);
    DependencyEntry* successor = static_cast<DependencyEntry*>(neighbors.successor);

    tree_.insert_new(entry->lhs, entry);
    entry->right = successor;
    if (predecessor != nullptr)
      predecessor->right = entry;
    else
      first_ = entry;
    entries_.push_back(std::move(owned));
    topology_version_.fetch_add(1, std::memory_order_release);
    return entry;
  }

  uint64_t neighbor_value(const DependencyEntry* entry) const {
    if (kind_ != DependencyKind::OD || entry == nullptr || entry->rhs_counts.empty())
      return 0;
    const DependencyEntry* successor = entry->right;
    while (successor != nullptr && successor->rhs_counts.empty())
      successor = successor->right;
    if (successor == nullptr)
      return 0;
    return compare_rhs_bytes(entry->rhs_max().data, entry->rhs_max().size, successor->rhs_min().data,
                             successor->rhs_min().size) > 0
               ? 1
               : 0;
  }

  void recompute_unlocked() {
    int64_t total = 0;
    for (DependencyEntry* entry = first_; entry != nullptr; entry = entry->right) {
      entry->local_violations = local_of(entry->distinct_rhs());
      entry->neighbor_violation = neighbor_value(entry);
      total += static_cast<int64_t>(entry->local_violations + entry->neighbor_violation);
    }
    live_violations_.store(total, std::memory_order_relaxed);
  }

  BTreeCppAdapter tree_;
  const DependencyKind kind_;
  StableRhsStorage rhs_storage_;
  std::vector<std::unique_ptr<DependencyEntry>> entries_;
  DependencyEntry* first_ = nullptr;
  std::atomic<int64_t> live_violations_{0};
  VersionedViolationHistory history_;
  std::atomic<InternalCommitID> completed_commit_cid_;
  std::atomic<InternalCommitID> visible_commit_cid_;
  InternalCommitID visibility_frontier_ = INVALID_INTERNAL_COMMIT_ID;
  std::atomic<uint64_t> topology_version_{0};

  mutable std::shared_mutex creation_mutex_;
  mutable std::mutex rhs_storage_mutex_;
  mutable std::mutex registered_mutex_;
  mutable std::condition_variable registered_cv_;
  std::mutex registration_driver_mutex_;
  std::mutex completion_driver_mutex_;
  std::map<InternalCommitID, std::shared_ptr<CommitEnvelope>> registered_commits_;
  InternalCommitID next_registered_cid_ = INVALID_INTERNAL_COMMIT_ID + 1;
  InternalCommitID registration_frontier_ = INVALID_INTERNAL_COMMIT_ID;
  CommitIDSource commit_id_source_ = CommitIDSource::Unset;
  std::atomic<std::size_t> prepared_bytes_current_{0};
  std::atomic<std::size_t> prepared_bytes_peak_{0};
#ifdef DV_TESTING
  std::atomic<uint64_t> od_active_transactions_{0};
  std::atomic<uint64_t> od_max_active_transactions_{0};
  std::atomic<uint64_t> optimistic_reprepare_count_{0};
  std::atomic<uint64_t> reservation_wait_count_{0};
#endif
};

#ifdef DV_TESTING
inline std::size_t DVTreeImpl::prepared_key_count_for_test(InternalCommitID cid) const {
  const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
  const auto found = registered_commits_.find(cid);
  return found == registered_commits_.end() ? 0 : found->second->fd_prepared_by_key.size();
}

inline bool DVTreeImpl::effects_installed_for_test(InternalCommitID cid) const {
  const auto registry_guard = std::lock_guard<std::mutex>{registered_mutex_};
  const auto found = registered_commits_.find(cid);
  return found != registered_commits_.end() && found->second->effects_installed;
}
#endif

inline DVTreeImpl::CommitTicket DVTreeImpl::begin_commit() {
  std::shared_ptr<CommitEnvelope> envelope;
  {
    const auto guard = std::lock_guard<std::mutex>{registered_mutex_};
    if (commit_id_source_ == CommitIDSource::External) {
      throw CommitOrderViolation("cannot mix internally and externally allocated commit ids");
    }
    commit_id_source_ = CommitIDSource::Internal;
    if (registered_commits_.empty()) {
      const InternalCommitID first_invisible = completed_commit_cid_.load(std::memory_order_acquire) + 1;
      next_registered_cid_ = std::max(next_registered_cid_, first_invisible);
    }
    if (next_registered_cid_ > MAX_VALID_INTERNAL_COMMIT_ID) {
      throw CommitOrderViolation("registered commit id reached Hyrise's reserved commit-id range");
    }
    const InternalCommitID cid = next_registered_cid_++;
    history_.reserve_append();
    try {
      envelope = std::make_shared<CommitEnvelope>(cid);
      const auto [_, inserted] = registered_commits_.emplace(cid, envelope);
      if (!inserted)
        throw CommitOrderViolation("registered commit id already exists");
    } catch (...) {
      history_.cancel_reserved_append();
      throw;
    }
    registration_frontier_ = cid;
  }
  registered_cv_.notify_all();
  return CommitTicket(this, std::move(envelope));
}

inline DVTreeImpl::CommitTicket DVTreeImpl::begin_commit(const InternalCommitID commit_id) {
  const auto guard = std::lock_guard<std::mutex>{registered_mutex_};
  if (commit_id_source_ == CommitIDSource::Internal) {
    throw CommitOrderViolation("cannot mix externally and internally allocated commit ids");
  }
  commit_id_source_ = CommitIDSource::External;
  if (commit_id == INVALID_INTERNAL_COMMIT_ID || commit_id > MAX_VALID_INTERNAL_COMMIT_ID) {
    throw CommitOrderViolation("external commit id is outside Hyrise's usable range");
  }
  if (commit_id <= completed_commit_cid_.load(std::memory_order_acquire)) {
    throw CommitOrderViolation("external commit id is not newer than this tree's completed state");
  }
  if (commit_id <= registration_frontier_) {
    throw CommitOrderViolation("external commit id is at or behind the closed registration frontier");
  }
  history_.reserve_append();
  auto envelope = std::shared_ptr<CommitEnvelope>{};
  try {
    envelope = std::make_shared<CommitEnvelope>(commit_id);
    const auto [_, inserted] = registered_commits_.emplace(commit_id, envelope);
    if (!inserted)
      throw CommitOrderViolation("external commit id is already registered");
  } catch (...) {
    history_.cancel_reserved_append();
    throw;
  }
  return CommitTicket(this, std::move(envelope));
}

inline void DVTreeImpl::advance_registration_frontier(const InternalCommitID commit_id) {
  {
    const auto guard = std::lock_guard<std::mutex>{registered_mutex_};
    if (commit_id_source_ == CommitIDSource::Internal) {
      throw CommitOrderViolation("the external registration frontier is unavailable with internal commit ids");
    }
    commit_id_source_ = CommitIDSource::External;
    if (commit_id == INVALID_INTERNAL_COMMIT_ID || commit_id > MAX_VALID_INTERNAL_COMMIT_ID) {
      throw CommitOrderViolation("registration frontier is outside Hyrise's usable range");
    }
    if (commit_id < registration_frontier_) {
      throw CommitOrderViolation("registration frontier cannot move backwards");
    }
    registration_frontier_ = commit_id;
  }
  registered_cv_.notify_all();
  process_ready_commits();
}

inline void DVTreeImpl::advance_visibility_frontier(const InternalCommitID commit_id) {
  {
    const auto guard = std::lock_guard<std::mutex>{registered_mutex_};
    if (commit_id_source_ == CommitIDSource::Internal) {
      throw CommitOrderViolation("the external visibility frontier is unavailable with internal commit ids");
    }
    commit_id_source_ = CommitIDSource::External;
    if (commit_id == INVALID_INTERNAL_COMMIT_ID || commit_id > MAX_VALID_INTERNAL_COMMIT_ID) {
      throw CommitOrderViolation("visibility frontier is outside Hyrise's usable range");
    }
    if (commit_id < visibility_frontier_) {
      // Monotonic no-op, not an error: Hyrise fires commit callbacks after a
      // lock-free watermark CAS, so the callback of a lower CID can lag behind
      // a higher CID's publish. The higher frontier already exposes the lower
      // CID's state -- its effects and history were installed before its rows
      // committed, which in turn preceded any higher CID's publication.
      return;
    }
    if (commit_id > registration_frontier_) {
      throw CommitOrderViolation("visibility frontier cannot pass the registration frontier");
    }
    visibility_frontier_ = commit_id;
    refresh_visible_commit_id_locked();
  }
  registered_cv_.notify_all();
}

inline InternalCommitID DVTreeImpl::CommitTicket::commit_id() const {
  if (!envelope_)
    throw CommitOrderViolation("commit ticket has no envelope");
  return envelope_->cid;
}

inline void DVTreeImpl::CommitTicket::stage(Transaction fragment) {
  ensure_staging();
  transaction_.ops.insert(transaction_.ops.end(), std::make_move_iterator(fragment.ops.begin()),
                          std::make_move_iterator(fragment.ops.end()));
}

inline void DVTreeImpl::CommitTicket::ensure_staging() const {
  if (resolved_ || owner_ == nullptr || !envelope_) {
    throw CommitOrderViolation("cannot stage into a resolved commit ticket");
  }
}

inline void DVTreeImpl::CommitTicket::seal() {
  if (resolved_ || owner_ == nullptr) {
    throw CommitOrderViolation("commit ticket is already resolved");
  }
  owner_->seal_registered(envelope_, std::move(transaction_));
  resolved_ = true;
}

inline void DVTreeImpl::CommitTicket::abort() {
  if (owner_ == nullptr || !envelope_)
    return;
  owner_->abort_registered(envelope_);
  resolved_ = true;
}

inline void DVTreeImpl::CommitTicket::wait_until_visible() const {
  if (!resolved_ || owner_ == nullptr || !envelope_) {
    throw CommitOrderViolation("commit ticket must be sealed or aborted before waiting");
  }
  owner_->wait_globally_visible(envelope_);
}

inline void DVTreeImpl::CommitTicket::wait_until_applied() const {
  if (!resolved_ || owner_ == nullptr || !envelope_) {
    throw CommitOrderViolation("commit ticket must be sealed or aborted before waiting");
  }
  owner_->wait_registered(envelope_);
}

inline void DVTreeImpl::CommitTicket::abandon_unresolved() noexcept {
  if (owner_ == nullptr || resolved_ || !envelope_)
    return;
  try {
    owner_->abort_registered(envelope_);
  } catch (...) {
    // Destructors cannot report commit cancellation failures. A caller that
    // needs the result must call abort() explicitly before destruction.
  }
  resolved_ = true;
}

}  // namespace hyrise::dv_tree
