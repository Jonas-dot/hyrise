#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

// Include the base OLC B-Tree
#include "../without_dependency_validation/btree_olc.hpp"

namespace btree_olc {

enum class DependencyType { FD, OD };

using CommitID = uint32_t;

// Versioned violation counter for snapshot-correct MVCC queries.
// Stores per-commit deltas; queries sum deltas with commit_id <= snapshot_cid.
// Writers hold the OLC write lock; readers use optimistic reads.
static constexpr uint32_t G_HISTORY_CAPACITY = 512;

struct GHistoryEntry {
  CommitID commit_id{0};
  int delta{0};
};

struct VersionedGHistory {
  mutable OptimisticLock _lock;
  uint32_t _size{0};
  int _total{0};  // running sum of all deltas -- O(1) latest query
  GHistoryEntry _buf[G_HISTORY_CAPACITY];

  // --- writer (called under write_lock) ---

  void _record(CommitID cid, int delta) {
    _total += delta;
    for (uint32_t i = 0; i < _size; ++i) {
      if (_buf[i].commit_id == cid) {
        _buf[i].delta += delta;
        return;
      }
    }
    if (_size < G_HISTORY_CAPACITY) {
      _buf[_size++] = {cid, delta};
    }
  }

  // --- reader (called under OLC read version) ---

  int _query(CommitID snapshot_cid) const {
    int g = 0;
    for (uint32_t i = 0; i < _size; ++i) {
      if (_buf[i].commit_id <= snapshot_cid) {
        g += _buf[i].delta;
      }
    }
    return g;
  }

  // --- public API ---

  void update(CommitID cid, int delta) {
    if (delta == 0) return;
    _lock.write_lock();
    _record(cid, delta);
    _lock.write_unlock();
  }

  int query(CommitID snapshot_cid) const {
    for (;;) {
      try {
        u64 v = _lock.read_lock_or_restart();
        int g = _query(snapshot_cid);
        _lock.check_or_restart(v);
        return g;
      } catch (const OLCRestartException&) {}
    }
  }

  int query_latest() const {
    for (;;) {
      try {
        u64 v = _lock.read_lock_or_restart();
        int g = _total;
        _lock.check_or_restart(v);
        return g;
      } catch (const OLCRestartException&) {}
    }
  }
};

struct MetadataDeltas {
  int flag_delta = 0;
  int local_delta = 0;

  int total() const {
    return flag_delta + local_delta;
  }

  void reset() {
    flag_delta = local_delta = 0;
  }

  MetadataDeltas& operator+=(const MetadataDeltas& o) {
    flag_delta += o.flag_delta;
    local_delta += o.local_delta;
    return *this;
  }
};

class DependencyValidatingBTree;

// Groups inserts/removes into a single commit. OD boundary violations are
// recomputed at commit time to avoid races on neighbor metadata.
//
// Usage:
//   DVTransactionContext txn(tree);
//   txn.insert(lhs, rhs);
//   txn.commit();
class DVTransactionContext {
 public:
  explicit DVTransactionContext(DependencyValidatingBTree& t) : _t(t) {}

  DVTransactionContext(const DVTransactionContext&) = delete;
  DVTransactionContext& operator=(const DVTransactionContext&) = delete;

  void insert(uint64_t det, uint64_t dep);
  void remove(uint64_t det, uint64_t dep);
  bool commit(CommitID commit_cid = CommitID{0});

  void rollback() {
    _pending.clear();
    _mod.clear();
    _ld = 0;
  }

  bool empty() const {
    return _pending.empty();
  }

 private:
  using PendingKey = std::pair<uint64_t, uint64_t>;

  DependencyValidatingBTree& _t;
  std::map<PendingKey, int64_t> _pending;
  std::set<uint64_t> _mod;
  int _ld = 0;
};

// Per-LHS payload stored in the B-Tree leaf.
// Layout: BTreeValueHeader followed by count * RHS_ENTRY_SIZE entries.
struct BTreeValueHeader {
  uint32_t count;
  uint32_t local_violation;
  uint32_t neighbor_flag;
  uint32_t _pad;
  uint64_t min_rhs;
  uint64_t max_rhs;
  // followed by count * sizeof(RhsEntry) entries (value + reference count)
};

static constexpr size_t BVH_SIZE = sizeof(BTreeValueHeader);

// Per-distinct-RHS entry after the BTreeValueHeader.  refcount tracks how many
// tuples contribute this (det, dep) pair so that removing a duplicate only
// decrements the count rather than dropping the entry.
struct RhsEntry {
  uint64_t value;
  uint32_t refcount;
  uint32_t _pad{0};
};

static constexpr size_t RHS_ENTRY_SIZE = sizeof(RhsEntry);

// OLC B-Tree with inline FD/OD violation metadata.
// Key = LHS (determinant); payload = BTreeValueHeader + rhs array.
// No external mutex: OLC write lock on each leaf covers all metadata updates.
class DependencyValidatingBTree {
 public:
  explicit DependencyValidatingBTree(DependencyType t) : _dep(t) {}

  // Safe unaligned payload reads (memcpy avoids SIGBUS on ARM64).
  // Use read_hdr/read_rhs for any read from a tree-node payload pointer;
  // direct reinterpret_cast is only safe on the alignas(8) TLS buffer.
  static BTreeValueHeader read_hdr(const u8* p) {
    BTreeValueHeader h;
    std::memcpy(&h, p, sizeof(h));
    return h;
  }

  static RhsEntry read_rhs_entry(const u8* p, uint32_t i) {
    RhsEntry e;
    std::memcpy(&e, p + BVH_SIZE + i * RHS_ENTRY_SIZE, sizeof(e));
    return e;
  }

  static uint64_t read_rhs(const u8* p, uint32_t i) {
    return read_rhs_entry(p, i).value;
  }

  MetadataDeltas insert(uint64_t det, uint64_t dep, CommitID commit_cid = CommitID{0}) {
    MetadataDeltas d;
    u8 lk[8];
    to_key(det, lk);
    bool refcount_bump = false;
    int old_lv = 0, new_lv = 0;
    bool new_entry = false;
    uint64_t old_max = 0, new_max = 0, new_min = 0;
    int od_delta = 0;

    auto mod = [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
      return _insert_mod(dep, op, ol, np, nl, refcount_bump, old_lv, new_lv, new_entry, old_max, new_max, new_min);
    };

    if (_dep == DependencyType::FD) {
      _tree.update_or_insert(lk, 8, mod);
    } else {
      _tree.update_with_neighbors(lk, 8, mod, [&](const u8* pp, u16 pl, const u8* sp, u16 sl) {
        if (refcount_bump)
          return;
        if (new_entry && pp && pl >= BVH_SIZE) {
          if (read_hdr(pp).max_rhs > new_min)
            od_delta += 1;
        }
        if (sp && sl >= BVH_SIZE) {
          auto sh = read_hdr(sp);
          bool ov = !new_entry && (old_max > sh.min_rhs);
          bool nv = (new_max > sh.min_rhs);
          if (nv && !ov)
            od_delta += 1;
          else if (!nv && ov)
            od_delta -= 1;
        }
      });
    }

    _total.fetch_add(1);
    if (refcount_bump)
      return d;
    d.local_delta = new_lv - old_lv;
    d.flag_delta = od_delta;
    _g_history.update(commit_cid, d.total());
    return d;
  }

  MetadataDeltas remove(uint64_t det, uint64_t dep, bool* removed = nullptr,
                        CommitID commit_cid = CommitID{0}) {
    MetadataDeltas d;
    u8 lk[8];
    to_key(det, lk);
    bool found = false, del_entry = false, refcount_only = false;
    int old_lv = 0, old_nf = 0, new_lv = 0;
    uint64_t old_max = 0, new_max = 0;

    _tree.update_or_insert(lk, 8, [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
      if (!op || ol < BVH_SIZE)
        return false;
      auto h = read_hdr(op);
      int ri = -1;
      for (uint32_t i = 0; i < h.count; ++i)
        if (read_rhs_entry(op, i).value == dep) {
          ri = i;
          break;
        }
      if (ri < 0)
        return false;
      found = true;
      old_lv = h.local_violation;
      old_nf = h.neighbor_flag;
      old_max = h.max_rhs;
      auto e = read_rhs_entry(op, ri);
      if (e.refcount > 1) {
        // More than one tuple contributes this value: decrement refcount only.
        // count, local_violation, and min/max are unchanged.
        std::memcpy(np, op, ol);
        reinterpret_cast<RhsEntry*>(np + BVH_SIZE)[ri].refcount--;
        nl = ol;
        new_max = old_max;
        refcount_only = true;
        return true;
      }
      // refcount == 1: last tuple with this value -- remove the entry.
      if (h.count <= 1) {
        del_entry = true;
        nl = 0;
        return false;
      }
      auto* nh = reinterpret_cast<BTreeValueHeader*>(np);
      auto* nr = reinterpret_cast<RhsEntry*>(np + BVH_SIZE);
      uint32_t j = 0;
      uint64_t mn = UINT64_MAX, mx = 0;
      for (uint32_t i = 0; i < h.count; ++i) {
        if (i != uint32_t(ri)) {
          auto ev = read_rhs_entry(op, i);
          nr[j++] = ev;
          mn = std::min(mn, ev.value);
          mx = std::max(mx, ev.value);
        }
      }
      nh->count = h.count - 1;
      nh->min_rhs = mn;
      nh->max_rhs = mx;
      nh->local_violation = nh->count - 1;
      nh->neighbor_flag = old_nf;
      nh->_pad = 0;
      new_lv = nh->local_violation;
      new_max = mx;
      nl = u16(BVH_SIZE + nh->count * RHS_ENTRY_SIZE);
      return true;
    });

    if (!found && !del_entry) {
      if (removed)
        *removed = false;
      return d;
    }
    if (removed)
      *removed = true;
    _total.fetch_sub(1);
    if (refcount_only)
      return d;
    if (del_entry) {
      d.local_delta = -old_lv;
      d.flag_delta = -old_nf;
      _tree.remove(lk, 8);
    } else {
      d.local_delta = new_lv - old_lv;
      if (_dep == DependencyType::OD && new_max != old_max)
        d.flag_delta = _od_update_remove(det, new_max, old_max);
    }
    _g_history.update(commit_cid, d.total());
    return d;
  }

  bool lookup(uint64_t det, uint64_t dep) const {
    u8 lk[8];
    to_key(det, lk);
    bool f = false;
    _tree.lookup(lk, 8, [&](const u8* p, u16 l) {
      if (l >= BVH_SIZE) {
        auto h = read_hdr(p);
        for (uint32_t i = 0; i < h.count; ++i)
          if (read_rhs(p, i) == dep) {
            f = true;
            break;
          }
      }
    });
    return f;
  }

  [[nodiscard]] int global_violation_count() const {
    return _g_history.query_latest();
  }

  [[nodiscard]] int global_violation_count(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid);
  }

  [[nodiscard]] bool dependency_holds() const {
    return _g_history.query_latest() == 0;
  }

  [[nodiscard]] bool dependency_holds(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid) == 0;
  }

  [[nodiscard]] size_t size() const {
    return _total.load();
  }

  static void to_key(uint64_t v, u8* buf) {
    for (int i = 7; i >= 0; --i)
      buf[7 - i] = u8((v >> (i * 8)) & 0xFF);
  }

  static uint64_t from_key(const u8* buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
      v = (v << 8) | buf[i];
    return v;
  }

 private:
  DependencyType _dep;
  BTree _tree;
  VersionedGHistory _g_history;
  std::atomic<size_t> _total{0};

  friend class DVTransactionContext;

  bool _insert_mod(uint64_t dep, const u8* op, u16 ol, u8* np, u16& nl, bool& refcount_bump, int& old_lv, int& new_lv,
                   bool& new_entry, uint64_t& old_max, uint64_t& new_max, uint64_t& new_min) {
    if (op && ol >= BVH_SIZE) {
      auto h = read_hdr(op);
      for (uint32_t i = 0; i < h.count; ++i) {
        auto e = read_rhs_entry(op, i);
        if (e.value == dep) {
          // Existing distinct value: bump its reference count only.
          // count, local_violation, and min/max are unchanged.
          std::memcpy(np, op, ol);
          reinterpret_cast<RhsEntry*>(np + BVH_SIZE)[i].refcount++;
          nl = ol;
          old_max = h.max_rhs;
          new_max = h.max_rhs;
          new_min = h.min_rhs;
          refcount_bump = true;
          return true;
        }
      }
      // New distinct value: append a fresh RhsEntry with refcount = 1.
      old_lv = h.local_violation;
      old_max = h.max_rhs;
      auto* nh = reinterpret_cast<BTreeValueHeader*>(np);
      auto* ne = reinterpret_cast<RhsEntry*>(np + BVH_SIZE);
      std::memcpy(ne, op + BVH_SIZE, h.count * RHS_ENTRY_SIZE);
      ne[h.count] = {dep, 1, 0};
      nh->count = h.count + 1;
      nh->min_rhs = std::min(h.min_rhs, dep);
      nh->max_rhs = std::max(h.max_rhs, dep);
      nh->local_violation = nh->count - 1;
      nh->neighbor_flag = h.neighbor_flag;
      nh->_pad = 0;
      new_lv = nh->local_violation;
      new_max = nh->max_rhs;
      new_min = nh->min_rhs;
      nl = u16(BVH_SIZE + nh->count * RHS_ENTRY_SIZE);
    } else {
      new_entry = true;
      auto* nh = reinterpret_cast<BTreeValueHeader*>(np);
      auto* ne = reinterpret_cast<RhsEntry*>(np + BVH_SIZE);
      nh->count = 1;
      nh->min_rhs = dep;
      nh->max_rhs = dep;
      nh->local_violation = 0;
      nh->neighbor_flag = 0;
      nh->_pad = 0;
      ne[0] = {dep, 1, 0};
      new_lv = 0;
      new_max = dep;
      new_min = dep;
      nl = u16(BVH_SIZE + RHS_ENTRY_SIZE);
    }
    return true;
  }

  int _od_update_remove(uint64_t det, uint64_t new_max, uint64_t old_max) {
    int d = 0;
    u8 lk[8];
    to_key(det, lk);
    uint64_t nm = UINT64_MAX;
    bool fn = false;
    _tree.find_successor(
        lk, 8, [](u64) {},
        [&](const u8* p, u16 l) {
          if (l >= BVH_SIZE) {
            nm = read_hdr(p).min_rhs;
            fn = true;
          }
        });
    if (fn) {
      bool ov = (old_max > nm), nv = (new_max > nm);
      if (!nv && ov) {
        _incr_nflag(det, -1);
        d = -1;
      } else if (nv && !ov) {
        _incr_nflag(det, 1);
        d = 1;
      }
    }
    return d;
  }

  void _incr_nflag(uint64_t det, int delta) {
    u8 lk[8];
    to_key(det, lk);
    _tree.update_or_insert(lk, 8, [delta](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
      if (!op || ol < BVH_SIZE)
        return false;
      std::memcpy(np, op, ol);
      auto* h = reinterpret_cast<BTreeValueHeader*>(np);
      if (delta > 0)
        h->neighbor_flag += uint32_t(delta);
      else if (delta < 0 && h->neighbor_flag >= uint32_t(-delta))
        h->neighbor_flag -= uint32_t(-delta);
      nl = ol;
      return true;
    });
  }

  bool _insert_deferred(uint64_t det, uint64_t dep, int& ld, uint64_t& pred_key_out, bool& has_pred) {
    u8 lk[8];
    to_key(det, lk);
    bool refcount_bump = false;
    int old_lv = 0, new_lv = 0;
    bool ne = false;
    uint64_t om = 0, nm = 0, nmn = 0;
    has_pred = false;

    _tree.update_with_neighbor_keys(lk, 8,
      [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
        return _insert_mod(dep, op, ol, np, nl, refcount_bump, old_lv, new_lv, ne, om, nm, nmn);
      },
      [&](const u8* pk, u16 pkl, const u8*, u16, const u8*, u16, const u8*, u16) {
        if (pk && pkl == 8) {
          pred_key_out = from_key(pk);
          has_pred = true;
        }
      });

    _total.fetch_add(1);
    if (refcount_bump) {
      ld = 0;
      return false;  // no structural change; key need not join the mod-set
    }
    ld = new_lv - old_lv;
    return true;
  }

  bool _remove_deferred(uint64_t det, uint64_t dep, int& ld, int& old_nf, uint64_t& pred_key_out, bool& has_pred) {
    u8 lk[8];
    to_key(det, lk);
    bool found = false, del = false, refcount_only = false;
    int old_lv = 0, new_lv = 0;
    old_nf = 0;
    has_pred = false;

    _tree.update_with_neighbor_keys(lk, 8, [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
      if (!op || ol < BVH_SIZE)
        return false;
      auto h = read_hdr(op);
      int ri = -1;
      for (uint32_t i = 0; i < h.count; ++i)
        if (read_rhs_entry(op, i).value == dep) {
          ri = i;
          break;
        }
      if (ri < 0)
        return false;
      found = true;
      old_lv = h.local_violation;
      old_nf = h.neighbor_flag;
      auto e = read_rhs_entry(op, ri);
      if (e.refcount > 1) {
        std::memcpy(np, op, ol);
        reinterpret_cast<RhsEntry*>(np + BVH_SIZE)[ri].refcount--;
        nl = ol;
        new_lv = old_lv;
        refcount_only = true;
        return true;
      }
      if (h.count <= 1) {
        del = true;
        return false;
      }
      auto* nh = reinterpret_cast<BTreeValueHeader*>(np);
      auto* nr = reinterpret_cast<RhsEntry*>(np + BVH_SIZE);
      uint32_t j = 0;
      uint64_t mn = UINT64_MAX, mx = 0;
      for (uint32_t i = 0; i < h.count; ++i)
        if (i != uint32_t(ri)) {
          auto ev = read_rhs_entry(op, i);
          nr[j++] = ev;
          mn = std::min(mn, ev.value);
          mx = std::max(mx, ev.value);
        }
      nh->count = h.count - 1;
      nh->min_rhs = mn;
      nh->max_rhs = mx;
      nh->local_violation = nh->count - 1;
      nh->neighbor_flag = old_nf;
      nh->_pad = 0;
      new_lv = nh->local_violation;
      nl = u16(BVH_SIZE + nh->count * RHS_ENTRY_SIZE);
      return true;
    },
    [&](const u8* pk, u16 pkl, const u8*, u16, const u8*, u16, const u8*, u16) {
      if (pk && pkl == 8) {
        pred_key_out = from_key(pk);
        has_pred = true;
      }
    });
    if (!found && !del) {
      ld = 0;
      return false;
    }
    _total.fetch_sub(1);
    if (refcount_only) {
      ld = 0;
      return false;  // no structural change; key need not join the mod-set
    }
    if (del) {
      ld = -old_lv;
      _tree.remove(lk, 8);
    } else {
      ld = new_lv - old_lv;
    }
    return true;
  }

  int _compute_bv(uint64_t det) const {
    u8 lk[8];
    to_key(det, lk);
    uint64_t my_max = 0;
    bool fs = false;
    _tree.lookup(lk, 8, [&](const u8* p, u16 l) {
      if (l >= BVH_SIZE) {
        my_max = read_hdr(p).max_rhs;
        fs = true;
      }
    });
    if (!fs)
      return 0;
    uint64_t nm = UINT64_MAX;
    bool fn = false;
    _tree.find_successor(
        lk, 8, [](u64) {},
        [&](const u8* p, u16 l) {
          if (l >= BVH_SIZE) {
            nm = read_hdr(p).min_rhs;
            fn = true;
          }
        });
    return (fn && my_max > nm) ? 1 : 0;
  }

  int _update_nflag(uint64_t det, int nf) {
    u8 lk[8];
    to_key(det, lk);
    int delta = 0;
    _tree.update_or_insert(lk, 8, [&delta, nf](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
      if (!op || ol < BVH_SIZE)
        return false;
      std::memcpy(np, op, ol);
      auto* h = reinterpret_cast<BTreeValueHeader*>(np);
      delta = nf - int(h->neighbor_flag);
      h->neighbor_flag = uint32_t(nf);
      nl = ol;
      return true;
    });
    return delta;
  }

  int _recompute_nflag(uint64_t det) {
    u8 lk[8];
    to_key(det, lk);
    uint64_t my_max = 0;
    int old_nf = 0;
    int delta = 0;
    u8* new_hdr_ptr = nullptr;  // will point into the modifier's output buffer

    _tree.update_with_neighbor_keys(lk, 8,
      [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
        if (!op || ol < BVH_SIZE) return false;
        auto h = read_hdr(op);
        my_max = h.max_rhs;
        old_nf = static_cast<int>(h.neighbor_flag);
        std::memcpy(np, op, ol);
        nl = ol;
        new_hdr_ptr = np;  // capture for the inspector to patch
        return true;
      },
      [&](const u8*, u16, const u8*, u16,
          const u8*, u16, const u8* sp, u16 spl) {
        uint64_t succ_min = UINT64_MAX;
        if (sp && spl >= BVH_SIZE)
          succ_min = read_hdr(sp).min_rhs;
        int new_nf = (succ_min != UINT64_MAX && my_max > succ_min) ? 1 : 0;
        delta = new_nf - old_nf;
        // Patch the flag in the output buffer before the B+-Tree commits it to the node.
        if (new_hdr_ptr)
          reinterpret_cast<BTreeValueHeader*>(new_hdr_ptr)->neighbor_flag = uint32_t(new_nf);
      });

    return delta;
  }
};

// DVTransactionContext implementation

inline void DVTransactionContext::insert(uint64_t det, uint64_t dep) {
  const auto key = PendingKey{det, dep};
  auto& delta = _pending[key];
  ++delta;
  if (delta == 0) {
    _pending.erase(key);
  }
}

inline void DVTransactionContext::remove(uint64_t det, uint64_t dep) {
  const auto key = PendingKey{det, dep};
  auto& delta = _pending[key];
  --delta;
  if (delta == 0) {
    _pending.erase(key);
  }
}

inline bool DVTransactionContext::commit(CommitID commit_cid) {
  auto apply_insert = [&](uint64_t det, uint64_t dep) {
    int ld = 0;
    uint64_t pred_key = 0;
    bool has_pred = false;
    if (_t._insert_deferred(det, dep, ld, pred_key, has_pred)) {
      _mod.insert(det);
      _ld += ld;
      if (_t._dep == DependencyType::OD && has_pred)
        _mod.insert(pred_key);
    }
  };

  auto apply_remove = [&](uint64_t det, uint64_t dep) {
    int ld = 0, onf = 0;
    uint64_t pred_key = 0;
    bool has_pred = false;
    if (_t._remove_deferred(det, dep, ld, onf, pred_key, has_pred)) {
      _mod.insert(det);
      _ld += ld;
      if (_t._dep == DependencyType::OD && has_pred)
        _mod.insert(pred_key);
    }
  };

  for (const auto& [key, delta] : _pending) {
    if (delta > 0) {
      for (int64_t i = 0; i < delta; ++i) {
        apply_insert(key.first, key.second);
      }
    } else {
      for (int64_t i = 0; i < -delta; ++i) {
        apply_remove(key.first, key.second);
      }
    }
  }

  int tot = _ld;
  if (_t._dep == DependencyType::OD) {
    for (uint64_t lhs : _mod)
      tot += _t._recompute_nflag(lhs);
  }
  _t._g_history.update(commit_cid, tot);
  _pending.clear();
  _mod.clear();
  _ld = 0;
  return true;
}

// Multi-column DV-Tree (standalone, tuple-aware)

class MultiColumnDependencyValidatingBTree {
 public:
  using Tuple = std::vector<uint64_t>;

  explicit MultiColumnDependencyValidatingBTree(DependencyType dep_type) : _dep(dep_type) {}

  MetadataDeltas insert(const Tuple& determinant, const Tuple& dependent,
                        CommitID commit_cid = CommitID{0}) {
    MetadataDeltas deltas;
    if (determinant.empty() || dependent.empty())
      return deltas;

    const auto lhs_key = _serialize_lhs_key(determinant);
    const auto rhs_tuple = _serialize_rhs_tuple(dependent);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    bool was_duplicate = false;
    int old_local = 0;
    int new_local = 0;
    bool was_new_entry = false;
    std::vector<u8> old_max;
    std::vector<u8> new_min;
    std::vector<u8> new_max;
    int od_boundary_delta = 0;

    auto modifier = [&](const u8* old_payload, u16 old_len, u8* new_payload, u16& new_len) -> bool {
      return _insert_modifier_tuple(rhs_tuple, tuple_width, old_payload, old_len, new_payload, new_len, was_duplicate,
                                    old_local, new_local, was_new_entry, old_max, new_min, new_max);
    };

    if (_dep == DependencyType::FD) {
      _tree.update_or_insert(lhs_key.data(), static_cast<u16>(lhs_key.size()), modifier);
    } else {
      _tree.update_with_neighbors(
          lhs_key.data(), static_cast<u16>(lhs_key.size()), modifier,
          [&](const u8* pred_payload, u16 pred_len, const u8* succ_payload, u16 succ_len) {
            if (was_duplicate)
              return;

            if (was_new_entry && pred_payload) {
              const auto pred_max = _read_max_tuple(pred_payload, pred_len);
              if (!pred_max.empty() && _tuple_cmp(pred_max.data(), new_min.data(), tuple_width) > 0) {
                od_boundary_delta += 1;
              }
            }

            if (succ_payload) {
              const auto succ_min = _read_min_tuple(succ_payload, succ_len);
              if (!succ_min.empty()) {
                bool old_violation = !was_new_entry && !old_max.empty() &&
                                     (_tuple_cmp(old_max.data(), succ_min.data(), tuple_width) > 0);
                bool new_violation = !new_max.empty() && (_tuple_cmp(new_max.data(), succ_min.data(), tuple_width) > 0);
                if (new_violation && !old_violation)
                  od_boundary_delta += 1;
                if (!new_violation && old_violation)
                  od_boundary_delta -= 1;
              }
            }
          });
    }

    if (was_duplicate) {
      _total.fetch_add(1);
      return deltas;
    }

    deltas.local_delta = new_local - old_local;
    deltas.flag_delta = od_boundary_delta;
    _total.fetch_add(1);
    _g_history.update(commit_cid, deltas.total());
    return deltas;
  }

  MetadataDeltas remove(const Tuple& determinant, const Tuple& dependent, bool* removed = nullptr,
                        CommitID commit_cid = CommitID{0}) {
    MetadataDeltas deltas;
    if (determinant.empty() || dependent.empty()) {
      if (removed)
        *removed = false;
      return deltas;
    }

    const auto lhs_key = _serialize_lhs_key(determinant);
    const auto rhs_tuple = _serialize_rhs_tuple(dependent);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    bool was_found = false;
    bool should_remove_entry = false;
    bool refcount_only = false;
    int old_local = 0;
    int new_local = 0;
    std::vector<u8> old_min;
    std::vector<u8> old_max;
    std::vector<u8> new_min;
    std::vector<u8> new_max;
    int od_boundary_delta = 0;

    auto remove_modifier = [&](const u8* old_payload, u16 old_len, u8* new_payload, u16& new_len) -> bool {
      if (!old_payload || old_len < sizeof(TuplePayloadHeader))
        return false;
      TuplePayloadHeader hdr;
      std::memcpy(&hdr, old_payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width)
        return false;
      const uint32_t stride = _entry_stride(tuple_width);
      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * stride;
      if (old_len < required_len)
        return false;

      int remove_pos = -1;
      const u8* tuples_base = old_payload + sizeof(TuplePayloadHeader);
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const u8* t = tuples_base + i * stride;
        if (_tuple_cmp(t, rhs_tuple.data(), tuple_width) == 0) {
          remove_pos = static_cast<int>(i);
          break;
        }
      }
      if (remove_pos < 0)
        return false;

      was_found = true;
      old_local = static_cast<int>(hdr.local_violation);

      // Scan for old min/max.
      const u8* min_ptr = tuples_base;
      const u8* max_ptr = tuples_base;
      for (uint32_t i = 1; i < hdr.count; ++i) {
        const u8* cur = tuples_base + i * stride;
        if (_tuple_cmp(cur, min_ptr, tuple_width) < 0)
          min_ptr = cur;
        if (_tuple_cmp(cur, max_ptr, tuple_width) > 0)
          max_ptr = cur;
      }
      old_min.assign(min_ptr, min_ptr + tuple_width);
      old_max.assign(max_ptr, max_ptr + tuple_width);

      // Check refcount: if > 1, just decrement -- entry stays.
      uint32_t rc = _read_refcount(tuples_base, static_cast<uint32_t>(remove_pos), tuple_width);
      if (rc > 1) {
        std::memcpy(new_payload, old_payload, old_len);
        _write_refcount(new_payload + sizeof(TuplePayloadHeader), static_cast<uint32_t>(remove_pos), tuple_width,
                        rc - 1);
        new_len = old_len;
        // Metadata unchanged: count, local_violation, min/max all stay the same.
        new_local = old_local;
        new_min = old_min;
        new_max = old_max;
        refcount_only = true;
        return true;
      }

      // refcount == 1: last tuple with this value -- remove the entry.
      if (hdr.count <= 1) {
        should_remove_entry = true;
        // Copy old payload so update_with_neighbors proceeds to the neighbor callback.
        // The actual tree removal happens after the callback returns.
        std::memcpy(new_payload, old_payload, old_len);
        new_len = old_len;
        return true;
      }

      auto* new_hdr = reinterpret_cast<TuplePayloadHeader*>(new_payload);
      new_hdr->count = hdr.count - 1;
      new_hdr->tuple_width = hdr.tuple_width;
      new_hdr->neighbor_flag = hdr.neighbor_flag;
      new_hdr->local_violation = new_hdr->count > 0 ? new_hdr->count - 1 : 0;

      u8* out_base = new_payload + sizeof(TuplePayloadHeader);
      uint32_t out_idx = 0;
      for (uint32_t i = 0; i < hdr.count; ++i) {
        if (static_cast<int>(i) == remove_pos)
          continue;
        const u8* src = tuples_base + i * stride;
        std::memcpy(out_base + out_idx * stride, src, stride);
        ++out_idx;
      }

      new_local = static_cast<int>(new_hdr->local_violation);
      // Scan remaining entries for new min/max.
      const u8* nmin = out_base;
      const u8* nmax = out_base;
      for (uint32_t i = 1; i < new_hdr->count; ++i) {
        const u8* cur = out_base + i * stride;
        if (_tuple_cmp(cur, nmin, tuple_width) < 0)
          nmin = cur;
        if (_tuple_cmp(cur, nmax, tuple_width) > 0)
          nmax = cur;
      }
      new_min.assign(nmin, nmin + tuple_width);
      new_max.assign(nmax, nmax + tuple_width);
      new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + new_hdr->count * stride);
      return true;
    };

    if (_dep == DependencyType::OD) {
      _tree.update_with_neighbors(
          lhs_key.data(), static_cast<u16>(lhs_key.size()), remove_modifier,
          [&](const u8* pred_payload, u16 pred_len, const u8* succ_payload, u16 succ_len) {
            if (!was_found || refcount_only)
              return;

            const auto pred_max = _read_max_tuple(pred_payload, pred_len);
            const auto succ_min = _read_min_tuple(succ_payload, succ_len);

            int old_boundary_violations = 0;
            int new_boundary_violations = 0;

            if (!pred_max.empty() && !old_min.empty() && _tuple_cmp(pred_max.data(), old_min.data(), tuple_width) > 0) {
              ++old_boundary_violations;
            }

            if (should_remove_entry) {
              if (!pred_max.empty() && !succ_min.empty() &&
                  _tuple_cmp(pred_max.data(), succ_min.data(), tuple_width) > 0) {
                ++new_boundary_violations;
              }
            } else {
              if (!pred_max.empty() && !new_min.empty() &&
                  _tuple_cmp(pred_max.data(), new_min.data(), tuple_width) > 0) {
                ++new_boundary_violations;
              }
            }

            if (!old_max.empty() && !succ_min.empty() && _tuple_cmp(old_max.data(), succ_min.data(), tuple_width) > 0) {
              ++old_boundary_violations;
            }

            if (!should_remove_entry && !new_max.empty() && !succ_min.empty() &&
                _tuple_cmp(new_max.data(), succ_min.data(), tuple_width) > 0) {
              ++new_boundary_violations;
            }

            od_boundary_delta += (new_boundary_violations - old_boundary_violations);
          });
    } else {
      _tree.update_or_insert(lhs_key.data(), static_cast<u16>(lhs_key.size()), remove_modifier);
    }

    if (!was_found && !should_remove_entry) {
      if (removed)
        *removed = false;
      return deltas;
    }

    if (refcount_only) {
      // Only decremented refcount -- metadata unchanged, no delta.
      _total.fetch_sub(1);
      if (removed)
        *removed = true;
      return deltas;
    }

    if (should_remove_entry) {
      deltas.local_delta = -old_local;
      deltas.flag_delta = od_boundary_delta;
      _tree.remove(lhs_key.data(), static_cast<u16>(lhs_key.size()));
    } else {
      deltas.local_delta = new_local - old_local;
      deltas.flag_delta = od_boundary_delta;
    }

    _total.fetch_sub(1);
    _g_history.update(commit_cid, deltas.total());
    if (removed)
      *removed = true;
    return deltas;
  }

  bool lookup(const Tuple& determinant, const Tuple& dependent) const {
    if (determinant.empty() || dependent.empty())
      return false;
    const auto lhs_key = _serialize_lhs_key(determinant);
    const auto rhs_tuple = _serialize_rhs_tuple(dependent);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    bool found = false;
    _tree.lookup(lhs_key.data(), static_cast<u16>(lhs_key.size()), [&](const u8* payload, u16 len) {
      if (!payload || len < sizeof(TuplePayloadHeader))
        return;
      TuplePayloadHeader hdr;
      std::memcpy(&hdr, payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width)
        return;
      const uint32_t stride = _entry_stride(tuple_width);
      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * stride;
      if (len < required_len)
        return;
      const u8* base = payload + sizeof(TuplePayloadHeader);
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const u8* t = base + i * stride;
        if (_tuple_cmp(t, rhs_tuple.data(), tuple_width) == 0) {
          found = true;
          break;
        }
      }
    });
    return found;
  }

  MetadataDeltas insert(uint64_t determinant, uint64_t dependent, CommitID commit_cid = CommitID{0}) {
    return insert(Tuple{determinant}, Tuple{dependent}, commit_cid);
  }

  MetadataDeltas remove(uint64_t determinant, uint64_t dependent, bool* removed = nullptr,
                        CommitID commit_cid = CommitID{0}) {
    return remove(Tuple{determinant}, Tuple{dependent}, removed, commit_cid);
  }

  bool lookup(uint64_t determinant, uint64_t dependent) const {
    return lookup(Tuple{determinant}, Tuple{dependent});
  }

  [[nodiscard]] int global_violation_count() const {
    return _g_history.query_latest();
  }

  [[nodiscard]] int global_violation_count(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid);
  }

  [[nodiscard]] bool dependency_holds() const {
    return _g_history.query_latest() == 0;
  }

  [[nodiscard]] bool dependency_holds(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid) == 0;
  }

  [[nodiscard]] size_t size() const {
    return _total.load();
  }

 private:
  struct TuplePayloadHeader {
    uint32_t count;
    uint32_t local_violation;
    uint32_t neighbor_flag;
    uint32_t tuple_width;  // byte width of column values only (excl. refcount)
  };

  // Each RHS entry in the flat array is [tuple bytes (tuple_width)] [refcount (4 bytes)].
  static uint32_t _entry_stride(uint32_t tw) { return tw + sizeof(uint32_t); }

  static uint32_t _read_refcount(const u8* base, uint32_t i, uint32_t tw) {
    uint32_t rc;
    std::memcpy(&rc, base + i * _entry_stride(tw) + tw, sizeof(rc));
    return rc;
  }

  static void _write_refcount(u8* base, uint32_t i, uint32_t tw, uint32_t rc) {
    std::memcpy(base + i * _entry_stride(tw) + tw, &rc, sizeof(rc));
  }

  static int _tuple_cmp(const u8* a, const u8* b, uint32_t width) {
    return std::memcmp(a, b, width);
  }

  static std::vector<u8> _serialize_lhs_key(const Tuple& lhs) {
    std::vector<u8> out;
    out.resize(2 + lhs.size() * sizeof(uint64_t));
    out[0] = static_cast<u8>((lhs.size() >> 8) & 0xFF);
    out[1] = static_cast<u8>(lhs.size() & 0xFF);
    size_t pos = 2;
    for (uint64_t v : lhs) {
      for (int i = 7; i >= 0; --i) {
        out[pos++] = static_cast<u8>((v >> (i * 8)) & 0xFF);
      }
    }
    return out;
  }

  static std::vector<u8> _serialize_rhs_tuple(const Tuple& rhs) {
    std::vector<u8> out;
    out.resize(rhs.size() * sizeof(uint64_t));
    size_t pos = 0;
    for (uint64_t v : rhs) {
      for (int i = 7; i >= 0; --i) {
        out[pos++] = static_cast<u8>((v >> (i * 8)) & 0xFF);
      }
    }
    return out;
  }

  static std::vector<u8> _read_min_tuple(const u8* payload, u16 len) {
    if (!payload || len < sizeof(TuplePayloadHeader))
      return {};
    TuplePayloadHeader hdr;
    std::memcpy(&hdr, payload, sizeof(hdr));
    if (hdr.count == 0 || hdr.tuple_width == 0)
      return {};
    const uint32_t stride = _entry_stride(hdr.tuple_width);
    const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * stride;
    if (len < required_len)
      return {};
    const u8* base = payload + sizeof(TuplePayloadHeader);
    const u8* min_ptr = base;
    for (uint32_t i = 1; i < hdr.count; ++i) {
      const u8* cur = base + i * stride;
      if (_tuple_cmp(cur, min_ptr, hdr.tuple_width) < 0)
        min_ptr = cur;
    }
    return std::vector<u8>(min_ptr, min_ptr + hdr.tuple_width);
  }

  static std::vector<u8> _read_max_tuple(const u8* payload, u16 len) {
    if (!payload || len < sizeof(TuplePayloadHeader))
      return {};
    TuplePayloadHeader hdr;
    std::memcpy(&hdr, payload, sizeof(hdr));
    if (hdr.count == 0 || hdr.tuple_width == 0)
      return {};
    const uint32_t stride = _entry_stride(hdr.tuple_width);
    const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * stride;
    if (len < required_len)
      return {};
    const u8* base = payload + sizeof(TuplePayloadHeader);
    const u8* max_ptr = base;
    for (uint32_t i = 1; i < hdr.count; ++i) {
      const u8* cur = base + i * stride;
      if (_tuple_cmp(cur, max_ptr, hdr.tuple_width) > 0)
        max_ptr = cur;
    }
    return std::vector<u8>(max_ptr, max_ptr + hdr.tuple_width);
  }

  static bool _insert_modifier_tuple(const std::vector<u8>& rhs_tuple, uint32_t tuple_width, const u8* old_payload,
                                     u16 old_len, u8* new_payload, u16& new_len, bool& was_duplicate, int& old_local,
                                     int& new_local, bool& was_new_entry, std::vector<u8>& old_max,
                                     std::vector<u8>& new_min, std::vector<u8>& new_max) {
    const uint32_t stride = _entry_stride(tuple_width);

    if (old_payload && old_len >= sizeof(TuplePayloadHeader)) {
      TuplePayloadHeader hdr;
      std::memcpy(&hdr, old_payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width)
        return false;
      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * stride;
      if (old_len < required_len)
        return false;

      old_local = static_cast<int>(hdr.local_violation);
      const u8* old_base = old_payload + sizeof(TuplePayloadHeader);

      // Scan for old min/max and check for duplicate.
      const u8* min_ptr = old_base;
      const u8* max_ptr = old_base;
      int dup_pos = -1;
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const u8* cur = old_base + i * stride;
        if (_tuple_cmp(cur, rhs_tuple.data(), tuple_width) == 0)
          dup_pos = static_cast<int>(i);
        if (_tuple_cmp(cur, min_ptr, tuple_width) < 0)
          min_ptr = cur;
        if (_tuple_cmp(cur, max_ptr, tuple_width) > 0)
          max_ptr = cur;
      }
      old_max.assign(max_ptr, max_ptr + tuple_width);

      if (dup_pos >= 0) {
        // Existing entry: bump its refcount only.
        was_duplicate = true;
        std::memcpy(new_payload, old_payload, old_len);
        const auto old_rc = _read_refcount(new_payload + sizeof(TuplePayloadHeader), dup_pos, tuple_width);
        _write_refcount(new_payload + sizeof(TuplePayloadHeader), dup_pos, tuple_width, old_rc + 1);
        new_len = old_len;
        return true;
      }

      // New distinct value: append at end.
      auto* out_hdr = reinterpret_cast<TuplePayloadHeader*>(new_payload);
      out_hdr->count = hdr.count + 1;
      out_hdr->tuple_width = hdr.tuple_width;
      out_hdr->neighbor_flag = hdr.neighbor_flag;
      out_hdr->local_violation = out_hdr->count > 0 ? out_hdr->count - 1 : 0;

      u8* out_base = new_payload + sizeof(TuplePayloadHeader);
      std::memcpy(out_base, old_base, hdr.count * stride);
      std::memcpy(out_base + hdr.count * stride, rhs_tuple.data(), tuple_width);
      const uint32_t rc_one = 1;
      std::memcpy(out_base + hdr.count * stride + tuple_width, &rc_one, sizeof(rc_one));

      new_local = static_cast<int>(out_hdr->local_violation);
      // new min/max = min/max(old, inserted)
      if (_tuple_cmp(rhs_tuple.data(), min_ptr, tuple_width) < 0)
        new_min = rhs_tuple;
      else
        new_min.assign(min_ptr, min_ptr + tuple_width);
      if (_tuple_cmp(rhs_tuple.data(), max_ptr, tuple_width) > 0)
        new_max = rhs_tuple;
      else
        new_max.assign(max_ptr, max_ptr + tuple_width);
      new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + out_hdr->count * stride);
      return true;
    }

    was_new_entry = true;
    auto* out_hdr = reinterpret_cast<TuplePayloadHeader*>(new_payload);
    out_hdr->count = 1;
    out_hdr->local_violation = 0;
    out_hdr->neighbor_flag = 0;
    out_hdr->tuple_width = tuple_width;
    u8* out_base = new_payload + sizeof(TuplePayloadHeader);
    std::memcpy(out_base, rhs_tuple.data(), tuple_width);
    const uint32_t rc_one = 1;
    std::memcpy(out_base + tuple_width, &rc_one, sizeof(rc_one));
    new_local = 0;
    new_min = rhs_tuple;
    new_max = rhs_tuple;
    new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + stride);
    return true;
  }

  int _od_delta_after_max_change(const std::vector<u8>& lhs_key, const std::vector<u8>& old_max,
                                 const std::vector<u8>& new_max, uint32_t tuple_width) {
    int delta = 0;
    std::vector<u8> succ_min;
    const bool has_succ = _tree.find_successor(
        lhs_key.data(), static_cast<u16>(lhs_key.size()), [](u64) {},
        [&](const u8* payload, u16 len) {
          succ_min = _read_min_tuple(payload, len);
        });
    if (!has_succ || succ_min.empty())
      return 0;

    const bool old_violation = _tuple_cmp(old_max.data(), succ_min.data(), tuple_width) > 0;
    const bool new_violation = _tuple_cmp(new_max.data(), succ_min.data(), tuple_width) > 0;
    if (!new_violation && old_violation)
      delta -= 1;
    if (new_violation && !old_violation)
      delta += 1;
    return delta;
  }

  DependencyType _dep;
  BTree _tree;
  VersionedGHistory _g_history;
  std::atomic<size_t> _total{0};
};

}  // namespace btree_olc
