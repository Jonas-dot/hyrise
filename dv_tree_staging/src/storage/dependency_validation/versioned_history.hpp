#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/dependency_validation/dv_types.hpp"

namespace hyrise::dv_tree {

// SnapshotHorizonViolation is a public exception declared in dv_types.hpp
// (included above) because it escapes public DVTree snapshot queries.

struct HistoryOrderViolation : std::logic_error {
  using std::logic_error::logic_error;
};

struct HistoryEntry {
  InternalCommitID commit_id = INVALID_INTERNAL_COMMIT_ID;
  int64_t delta = 0;
  int64_t total_after = 0;
};

// A soft-bounded committed-delta history. Entries older than the DBMS-provided
// lowest-active snapshot are folded into baseline_. If the active snapshot
// window itself exceeds soft_capacity_, the vector grows instead of rejecting a
// commit. Advancing/clearing the horizon folds the excess again. In particular,
// folding is deliberately not performed by update_reserved(): final commit
// installation must not do unbounded maintenance work after its storage slot
// has been reserved.
//
// A shared mutex is deliberate: unlike a fixed atomic ring, a dynamically
// growing container cannot be read safely by a seqlock reader while it moves
// its bookkeeping. Queries remain concurrent with each other and completion
// remains a short exclusive operation.
class VersionedViolationHistory {
 public:
  struct MemoryStats {
    std::size_t retained_entries = 0;
    std::size_t soft_capacity = 0;
    std::size_t value_bytes = 0;
    bool has_lowest_active_snapshot = false;
    InternalCommitID lowest_active_snapshot = INVALID_INTERNAL_COMMIT_ID;
    bool has_evicted_history = false;
    InternalCommitID oldest_exact_snapshot = INVALID_INTERNAL_COMMIT_ID;
  };

  explicit VersionedViolationHistory(std::size_t soft_capacity) : soft_capacity_(soft_capacity) {
    if (soft_capacity == 0) {
      throw std::invalid_argument("history soft capacity must be positive");
    }
    entries_.reserve(soft_capacity_);
  }

  void update(InternalCommitID cid, int64_t delta) {
    reserve_append();
    update_reserved(cid, delta);
    // The standalone convenience API preserves its eager soft-cap behavior.
    // The Hyrise commit path deliberately uses reserve_append() followed by
    // update_reserved() directly, leaving trimming to a later horizon refresh.
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    trim_to_soft_capacity_locked();
  }

  // This is the only operation allowed to grow storage. Tickets call it while
  // they are still prepared/abortable; update_reserved() is non-allocating.
  void reserve_append() {
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    const auto required = entries_.size() + reserved_appends_ + 1;
    if (required > entries_.capacity()) {
      entries_.reserve(std::max(required, std::max(std::size_t{1}, entries_.capacity() * 2)));
    }
    ++reserved_appends_;
  }

  void update_reserved(InternalCommitID cid, int64_t delta) {
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    if (reserved_appends_ == 0)
      throw HistoryOrderViolation("history append was not reserved");
    --reserved_appends_;
    if (delta == 0)
      return;
    if (cid == INVALID_INTERNAL_COMMIT_ID)
      throw HistoryOrderViolation("commit id zero is reserved");
    record_reserved_locked(cid, delta);
  }

  void cancel_reserved_append() {
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    if (reserved_appends_ == 0)
      throw HistoryOrderViolation("history append reservation was already resolved");
    --reserved_appends_;
  }

  int64_t query(InternalCommitID snapshot) const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return query_locked(snapshot);
  }

  int64_t query_latest() const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return total_;
  }

  bool can_query_exactly(InternalCommitID snapshot) const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return can_query_exactly_locked(snapshot);
  }

  int64_t query_exact(InternalCommitID snapshot) const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    if (!can_query_exactly_locked(snapshot)) {
      throw SnapshotHorizonViolation("snapshot " + std::to_string(snapshot) +
                                     " has scrolled out of the exact history window");
    }
    return query_locked(snapshot);
  }

  void set_lowest_active(InternalCommitID cid) {
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    if (has_lowest_active_ && cid < lowest_active_) {
      throw HistoryOrderViolation("lowest-active snapshot cannot move backwards");
    }
    if (evicted_ && cid < max_evicted_cid_) {
      throw SnapshotHorizonViolation("lowest-active snapshot has already scrolled out");
    }
    lowest_active_ = cid;
    has_lowest_active_ = true;
    trim_to_soft_capacity_locked();
  }

  void clear_lowest_active() {
    auto guard = std::unique_lock<std::shared_mutex>{mutex_};
    has_lowest_active_ = false;
    trim_to_soft_capacity_locked();
  }

  std::size_t size() const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return entries_.size();
  }

  // The target retained size when the active snapshot horizon permits
  // folding. size() may temporarily exceed this soft capacity.
  std::size_t capacity() const {
    return soft_capacity_;
  }

  bool evicted() const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return evicted_;
  }

  MemoryStats memory_stats() const {
    const auto guard = std::shared_lock<std::shared_mutex>{mutex_};
    return {entries_.size(), soft_capacity_, entries_.size() * sizeof(HistoryEntry),         has_lowest_active_,
            lowest_active_,  evicted_,       evicted_ ? max_evicted_cid_ : INVALID_INTERNAL_COMMIT_ID};
  }

 private:
  bool can_query_exactly_locked(InternalCommitID snapshot) const {
    return !evicted_ || snapshot >= max_evicted_cid_;
  }

  bool oldest_is_evictable_locked() const {
    return !has_lowest_active_ || entries_.front().commit_id < lowest_active_;
  }

  void fold_oldest_locked() {
    const HistoryEntry oldest = entries_.front();
    baseline_ = oldest.total_after;
    max_evicted_cid_ = oldest.commit_id;
    evicted_ = true;
    entries_.erase(entries_.begin());
  }

  void trim_to_soft_capacity_locked() {
    while (entries_.size() > soft_capacity_ && oldest_is_evictable_locked()) {
      fold_oldest_locked();
    }
  }

  void record_reserved_locked(InternalCommitID cid, int64_t delta) {
    if (cid < last_cid_)
      throw HistoryOrderViolation("commit ids must be non-decreasing");

    const bool coalesces = !entries_.empty() && entries_.back().commit_id == cid;
    if (coalesces) {
      entries_.back().delta += delta;
      total_ += delta;
      entries_.back().total_after = total_;
      last_cid_ = cid;
      return;
    }

    // reserve_append() guaranteed this slot before installation.
    if (entries_.size() == entries_.capacity())
      throw HistoryOrderViolation("reserved history append unexpectedly exhausted capacity");
    const int64_t total_after = total_ + delta;
    entries_.push_back({cid, delta, total_after});
    total_ = total_after;
    last_cid_ = cid;
    // Trimming is intentionally deferred to a horizon refresh. Besides being
    // allocation-free, the irreversible completion path therefore performs
    // only a constant-time append/coalesce once reserve_append() succeeded.
  }

  int64_t query_locked(InternalCommitID snapshot) const {
    const auto after_snapshot =
        std::upper_bound(entries_.begin(), entries_.end(), snapshot, [](InternalCommitID cid, const HistoryEntry& entry) {
          return cid < entry.commit_id;
        });
    if (after_snapshot == entries_.begin())
      return evicted_ ? baseline_ : 0;
    return std::prev(after_snapshot)->total_after;
  }

  std::vector<HistoryEntry> entries_;
  const std::size_t soft_capacity_;
  std::size_t reserved_appends_ = 0;
  int64_t total_ = 0;
  int64_t baseline_ = 0;
  InternalCommitID max_evicted_cid_ = INVALID_INTERNAL_COMMIT_ID;
  bool evicted_ = false;
  InternalCommitID last_cid_ = INVALID_INTERNAL_COMMIT_ID;
  bool has_lowest_active_ = false;
  InternalCommitID lowest_active_ = INVALID_INTERNAL_COMMIT_ID;
  mutable std::shared_mutex mutex_;
};

}  // namespace hyrise::dv_tree
