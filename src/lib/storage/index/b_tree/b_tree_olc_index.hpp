#pragma once

// OLC B-Tree with dependency validation, integrated as a Hyrise AbstractChunkIndex.
// The B-Tree engine is ported from btree24 (Müller et al., SIGMOD 2025):
//   https://github.com/m-mueller678/btree24/tree/sigmod25
// Locking follows Optimistic Lock Coupling (Leis et al., 2019).
// All internal types live in namespace olc_detail to avoid collisions with hyrise::.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <thread>
#include <vector>

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "all_type_variant.hpp"
#include "storage/index/abstract_chunk_index.hpp"
#include "types.hpp"

namespace hyrise {

namespace olc_detail {

// Short-hand integer types ported from btree24.
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;

// PAGE_SIZE is kept only as an upper-bound for TLS key/payload scratch buffers.
// It is NOT used for struct sizing or alignment -- BTreeNode is plain (no alignas).
static constexpr size_t PAGE_SIZE = 4096;

struct OLCRestartException : public std::exception {
  const char* what() const noexcept override {
    return "OLC restart";
  }
};

inline void olc_yield(u64 c = 0) {
#ifdef __x86_64__
  _mm_pause();
#else
  std::this_thread::yield();
#endif
  if (c > 5) {
    thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<u64> dist(0, std::min(c * 5, u64{500}));
    std::this_thread::sleep_for(std::chrono::microseconds(dist(gen)));
  }
}

class OptimisticLock {
 public:
  static constexpr u64 UNLOCKED = 0;
  static constexpr u64 LOCKED = 253;

  OptimisticLock() : _s(0) {}

  OptimisticLock(const OptimisticLock&) = delete;
  OptimisticLock& operator=(const OptimisticLock&) = delete;

  u64 read_lock_or_restart() const {
    u64 v = _s.load(std::memory_order_acquire);
    if ((v >> 56) == LOCKED)
      throw OLCRestartException();
    return v;
  }

  bool validate(u64 v) const {
    std::atomic_thread_fence(std::memory_order_acquire);
    return _s.load(std::memory_order_relaxed) == v;
  }

  void check_or_restart(u64 v) const {
    if (!validate(v))
      throw OLCRestartException();
  }

  void write_lock() {
    for (u64 c = 0;; ++c) {
      u64 v = _s.load(std::memory_order_relaxed);
      if ((v >> 56) == UNLOCKED) {
        u64 lk = ((v << 8) >> 8) | (LOCKED << 56);
        if (_s.compare_exchange_weak(v, lk, std::memory_order_acquire))
          return;
      }
      olc_yield(c);
    }
  }

  bool try_write_lock(u64 exp) {
    if ((exp >> 56) != UNLOCKED)
      return false;
    u64 lk = ((exp << 8) >> 8) | (LOCKED << 56);
    return _s.compare_exchange_strong(exp, lk, std::memory_order_acquire);
  }

  void write_unlock() {
    u64 v = _s.load(std::memory_order_relaxed);
    _s.store((((v << 8) >> 8) + 1) | (UNLOCKED << 56), std::memory_order_release);
  }

 private:
  std::atomic<u64> _s;
};

struct BTreeNode;

class WriteGuard {
 public:
  WriteGuard() : _n(nullptr), _l(nullptr) {}

  explicit WriteGuard(BTreeNode* n);
  WriteGuard(BTreeNode* n, u64 exp);

  WriteGuard(WriteGuard&& o) noexcept : _n(o._n), _l(o._l) {
    o._n = nullptr;
    o._l = nullptr;
  }

  WriteGuard& operator=(WriteGuard&& o) noexcept {
    release();
    _n = o._n;
    _l = o._l;
    o._n = nullptr;
    o._l = nullptr;
    return *this;
  }

  ~WriteGuard() {
    release();
  }

  void release();

  BTreeNode* node() const {
    return _n;
  }

  BTreeNode* operator->() const {
    return _n;
  }

 private:
  BTreeNode* _n;
  OptimisticLock* _l;
};

struct BTreeNode {
  static constexpr u16 HINT_COUNT = 16;
  // UNDERFULL_SZ is used only by merge_nodes (merges currently disabled).
  static constexpr unsigned UNDERFULL_SZ = 3072;

  OptimisticLock lock;
  bool is_leaf;
  u8 _pad[3];
  u16 count, space_used, data_offset, prefix_len;

  struct FenceSlot {
    u16 offset, len;
  };

  FenceSlot lower_fence, upper_fence;
  u32 hints[HINT_COUNT];

  union {
    BTreeNode* upper_child;
    BTreeNode* next_leaf;
  };

  BTreeNode* prev_leaf;  // backward sibling pointer (leaf nodes only; null for inner)

  struct Slot {
    u16 offset, key_len, payload_len;
    u32 head;
  } __attribute__((packed));

  static constexpr size_t DATA_CAP = 3968;

  union {
    Slot slots[1];
    u8 data[DATA_CAP];
  };

  explicit BTreeNode(bool leaf)
      : is_leaf(leaf),
        count(0),
        space_used(0),
        data_offset(sizeof(BTreeNode)),
        prefix_len(0),
        lower_fence{0, 0},
        upper_fence{0, 0} {
    std::memset(hints, 0, sizeof(hints));
    upper_child = nullptr;
    prev_leaf   = nullptr;
  }

  u8* ptr() {
    return reinterpret_cast<u8*>(this);
  }

  const u8* ptr() const {
    return reinterpret_cast<const u8*>(this);
  }

  u8* get_key(u16 i) {
    return ptr() + slots[i].offset;
  }

  const u8* get_key(u16 i) const {
    return ptr() + slots[i].offset;
  }

  u8* get_payload(u16 i) {
    return get_key(i) + slots[i].key_len;
  }

  const u8* get_payload(u16 i) const {
    return get_key(i) + slots[i].key_len;
  }

  u8* get_lower_fence() {
    return ptr() + lower_fence.offset;
  }

  const u8* get_lower_fence() const {
    return ptr() + lower_fence.offset;
  }

  u8* get_upper_fence() {
    return ptr() + upper_fence.offset;
  }

  const u8* get_upper_fence() const {
    return ptr() + upper_fence.offset;
  }

  u8* get_prefix() {
    return get_lower_fence();
  }

  const u8* get_prefix() const {
    return get_lower_fence();
  }

  u16 free_space() const {
    return data_offset - u16(reinterpret_cast<const u8*>(&slots[count]) - ptr());
  }

  u16 free_space_after_compaction() const {
    return u16(sizeof(BTreeNode) - (reinterpret_cast<const u8*>(&slots[count]) - ptr()) - space_used);
  }

  u16 space_needed(u16 kl, u16 pl) const {
    return u16(sizeof(Slot) + (kl > prefix_len ? kl - prefix_len : 0) + pl);
  }

  bool has_space_for(u16 kl, u16 pl) const {
    return space_needed(kl, pl) <= free_space_after_compaction();
  }

  static u32 compute_head(const u8* k, u16 l) {
    switch (l) {
      case 0:
        return 0;
      case 1:
        return u32(k[0]) << 24;
      case 2:
        return (u32(k[0]) << 24) | (u32(k[1]) << 16);
      case 3:
        return (u32(k[0]) << 24) | (u32(k[1]) << 16) | (u32(k[2]) << 8);
      default:
        return (u32(k[0]) << 24) | (u32(k[1]) << 16) | (u32(k[2]) << 8) | u32(k[3]);
    }
  }

  void make_hints() {
    if (!count)
      return;
    u16 d = count / (HINT_COUNT + 1);
    for (u16 i = 0; i < HINT_COUNT; ++i)
      hints[i] = slots[d * (i + 1)].head;
  }

  void search_hints(u32 h, u16& lo, u16& hi) const {
    if (count > HINT_COUNT * 2) {
      u16 d = hi / (HINT_COUNT + 1), pos = 0;
      for (; pos < HINT_COUNT && hints[pos] < h; ++pos) {}
      u16 p2 = pos;
      for (; p2 < HINT_COUNT && hints[p2] == h; ++p2) {}
      lo = pos * d;
      if (p2 < HINT_COUNT)
        hi = (p2 + 1) * d;
    }
  }

  u16 lower_bound(const u8* key, u16 klen, bool& found) const {
    found = false;
    if (!count)
      return 0;
    int cmp = std::memcmp(key, get_prefix(), std::min(klen, prefix_len));
    if (cmp < 0)
      return 0;
    if (cmp > 0)
      return count;
    if (klen < prefix_len)
      return 0;
    const u8* sk = key + prefix_len;
    u16 slen = klen - prefix_len;
    u32 h = compute_head(sk, slen);
    u16 lo = 0, hi = count;
    search_hints(h, lo, hi);
    while (lo < hi) {
      u16 mid = lo + (hi - lo) / 2;
      if (h < slots[mid].head) {
        hi = mid;
      } else if (h > slots[mid].head) {
        lo = mid + 1;
      } else {
        int c = std::memcmp(sk, get_key(mid), std::min(slen, slots[mid].key_len));
        if (c < 0) {
          hi = mid;
        } else if (c > 0) {
          lo = mid + 1;
        } else if (slen < slots[mid].key_len) {
          hi = mid;
        } else if (slen > slots[mid].key_len) {
          lo = mid + 1;
        } else {
          found = true;
          return mid;
        }
      }
    }
    return lo;
  }

  u16 lower_bound(const u8* key, u16 klen) const {
    bool f;
    return lower_bound(key, klen, f);
  }

  BTreeNode* get_child(u16 i) const {
    BTreeNode* c;
    std::memcpy(&c, get_payload(i), sizeof(c));
    return c;
  }

  BTreeNode* lookup_inner(const u8* key, u16 klen) const {
    u16 pos = lower_bound(key, klen);
    return (pos == count) ? upper_child : get_child(pos);
  }

  void compactify();

  void insert_fence(FenceSlot& fk, const u8* key, u16 len) {
    assert(free_space() >= len);
    data_offset -= len;
    space_used += len;
    fk.offset = data_offset;
    fk.len = len;
    std::memcpy(ptr() + data_offset, key, len);
  }

  void set_fences(const u8* lo, u16 ll, const u8* up, u16 ul) {
    insert_fence(lower_fence, lo, ll);
    insert_fence(upper_fence, up, ul);
    for (prefix_len = 0; prefix_len < std::min(ll, ul) && lo[prefix_len] == up[prefix_len]; ++prefix_len) {}
  }

  void store_key_value(u16 i, const u8* key, u16 klen, const u8* payload, u16 plen) {
    const u8* k = key + prefix_len;
    u16 kl = klen - prefix_len;
    slots[i].head = compute_head(k, kl);
    slots[i].key_len = kl;
    slots[i].payload_len = plen;
    data_offset -= kl + plen;
    space_used += kl + plen;
    slots[i].offset = data_offset;
    std::memcpy(get_key(i), k, kl);
    std::memcpy(get_payload(i), payload, plen);
  }

  void insert_in_page(const u8* key, u16 klen, const u8* payload, u16 plen) {
    u16 need = space_needed(klen, plen);
    if (need > free_space()) {
      assert(need <= free_space_after_compaction());
      compactify();
    }
    bool found;
    u16 slot = lower_bound(key, klen, found);
    if (found) {
      space_used -= slots[slot].payload_len + slots[slot].key_len;
    } else {
      std::memmove(&slots[slot + 1], &slots[slot], sizeof(Slot) * (count - slot));
      ++count;
    }
    store_key_value(slot, key, klen, payload, plen);
    if (!found)
      make_hints();
  }

  bool remove_slot(u16 slot) {
    space_used -= slots[slot].key_len + slots[slot].payload_len;
    std::memmove(&slots[slot], &slots[slot + 1], sizeof(Slot) * (count - slot - 1));
    --count;
    make_hints();
    return true;
  }

  struct SepInfo {
    u16 length, slot;
    bool truncated;
  };

  u16 common_prefix(u16 a, u16 b) const {
    u16 lim = std::min(slots[a].key_len, slots[b].key_len);
    const u8 *ka = get_key(a), *kb = get_key(b);
    u16 i = 0;
    for (; i < lim && ka[i] == kb[i]; ++i) {}
    return i;
  }

  SepInfo find_separator() const {
    assert(count > 1);
    u16 slot = count / 2;
    if (!is_leaf) {
      return {u16(prefix_len + slots[slot].key_len), slot, false};
    }
    if (slot + 1 < count) {
      u16 cp = common_prefix(slot, slot + 1);
      if (slots[slot].key_len > cp && slots[slot + 1].key_len > cp + 1)
        return {u16(prefix_len + cp + 1), slot, true};
    }
    return {u16(prefix_len + slots[slot].key_len), slot, false};
  }

  void get_separator(u8* out, const SepInfo& info) const {
    std::memcpy(out, get_prefix(), prefix_len);
    std::memcpy(out + prefix_len, get_key(info.slot + (info.truncated ? 1 : 0)), info.length - prefix_len);
  }

  void copy_key_value_range(BTreeNode* dst, u16 dslot, u16 sslot, u16 scount);
  void split_node(BTreeNode* parent, u16 sep_slot, const u8* sep, u16 sep_len);
  bool merge_nodes(u16 slot_id, BTreeNode* parent, BTreeNode* right);

  static BTreeNode* alloc(bool leaf) {
    void* mem = nullptr;
#if defined(__APPLE__) || defined(_POSIX_VERSION)
    if (posix_memalign(&mem, 64, sizeof(BTreeNode)) != 0)
      throw std::bad_alloc();
#else
    mem = std::aligned_alloc(64, sizeof(BTreeNode));
    if (!mem)
      throw std::bad_alloc();
#endif
    return new (mem) BTreeNode(leaf);
  }

  static void dealloc(BTreeNode* n) {
    if (!n) return;
    if (!n->is_leaf) {
      for (u16 i = 0; i < n->count; ++i)
        dealloc(n->get_child(i));
      dealloc(n->upper_child);
    }
    n->~BTreeNode();
    std::free(n);
  }
};

inline WriteGuard::WriteGuard(BTreeNode* n) : _n(n), _l(&n->lock) {
  _l->write_lock();
}

inline WriteGuard::WriteGuard(BTreeNode* n, u64 exp) : _n(n), _l(&n->lock) {
  if (!_l->try_write_lock(exp)) {
    _n = nullptr;
    _l = nullptr;
    throw OLCRestartException();
  }
}

inline void WriteGuard::release() {
  if (_l) {
    _l->write_unlock();
    _l = nullptr;
    _n = nullptr;
  }
}

// Thread-local separator-key buffer, declared here for use in inline BTree methods.
extern thread_local u8 tls_btree_sep_buf[PAGE_SIZE];

class BTree {
 public:
  BTree() : _root(BTreeNode::alloc(true)), _size(0) {}

  ~BTree() {
    BTreeNode* r = _root.load(std::memory_order_relaxed);
    if (r)
      BTreeNode::dealloc(r);
  }

  // Non-copyable, non-movable (owning raw pointers)
  BTree(const BTree&) = delete;
  BTree& operator=(const BTree&) = delete;

  template <typename CB>
  bool lookup(const u8* key, u16 klen, CB cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, klen);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, klen, found);
        if (found)
          cb(node->get_payload(pos), node->slots[pos].payload_len);
        node->lock.check_or_restart(v);
        return found;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  template <typename Mod>
  bool update_or_insert(const u8* key, u16 klen, Mod mod) {
    alignas(8) thread_local u8 nbuf[PAGE_SIZE];
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, klen);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        WriteGuard lg(node, v);
        bool found;
        u16 pos = lg->lower_bound(key, klen, found);
        const u8* op = found ? lg->get_payload(pos) : nullptr;
        u16 ol = found ? lg->slots[pos].payload_len : 0;
        u16 nl = 0;
        if (!mod(op, ol, nbuf, nl))
          return false;
        if (found) {
          if (nl == ol) {
            std::memcpy(lg->get_payload(pos), nbuf, nl);
          } else {
            lg->remove_slot(pos);
            if (!lg->has_space_for(klen, nl)) {
              lg.release();
              return insert(key, klen, nbuf, nl);
            }
            lg->insert_in_page(key, klen, nbuf, nl);
          }
        } else {
          if (!lg->has_space_for(klen, nl)) {
            lg.release();
            return insert(key, klen, nbuf, nl);
          }
          lg->insert_in_page(key, klen, nbuf, nl);
          _size.fetch_add(1);
        }
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  template <typename Mod, typename NI>
  bool update_with_neighbors(const u8* key, u16 klen, Mod mod, NI ni) {
    alignas(8) thread_local u8 nbuf[PAGE_SIZE];
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* cur = _root.load(std::memory_order_acquire);
        while (!cur->is_leaf) {
          u64 cv = cur->lock.read_lock_or_restart();
          BTreeNode* child = cur->lookup_inner(key, klen);
          cur->lock.check_or_restart(cv);
          cur = child;
        }
        u64 v = cur->lock.read_lock_or_restart();
        WriteGuard lg(cur, v);
        bool found;
        u16 pos = lg->lower_bound(key, klen, found);
        const u8* op = found ? lg->get_payload(pos) : nullptr;
        u16 ol = found ? lg->slots[pos].payload_len : 0;
        u16 nl = 0;
        if (!mod(op, ol, nbuf, nl))
          return false;
        // neighbor inspection
        const u8 *pp = nullptr, *sp = nullptr;
        u16 pl = 0, sl = 0;
        if (found) {
          if (pos > 0) {
            pp = lg->get_payload(pos - 1);
            pl = lg->slots[pos - 1].payload_len;
          }
          if (pos + 1 < lg->count) {
            sp = lg->get_payload(pos + 1);
            sl = lg->slots[pos + 1].payload_len;
          }
        } else {
          if (pos > 0) {
            pp = lg->get_payload(pos - 1);
            pl = lg->slots[pos - 1].payload_len;
          }
          if (pos < lg->count) {
            sp = lg->get_payload(pos);
            sl = lg->slots[pos].payload_len;
          }
        }
        ni(pp, pl, sp, sl);
        if (found) {
          if (nl == ol) {
            std::memcpy(lg->get_payload(pos), nbuf, nl);
          } else {
            lg->remove_slot(pos);
            if (!lg->has_space_for(klen, nl)) {
              lg.release();
              return insert(key, klen, nbuf, nl);
            }
            lg->insert_in_page(key, klen, nbuf, nl);
          }
        } else {
          if (!lg->has_space_for(klen, nl)) {
            lg.release();
            return insert(key, klen, nbuf, nl);
          }
          lg->insert_in_page(key, klen, nbuf, nl);
          _size.fetch_add(1);
        }
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  // Like update_with_neighbors, but the inspector also receives predecessor/successor keys.
  // inspector(pred_key, pred_klen, pred_payload, pred_plen,
  //           succ_key, succ_klen, succ_payload, succ_plen)
  template <typename Mod, typename NKI>
  bool update_with_neighbor_keys(const u8* key, u16 klen, Mod mod, NKI inspector) {
    alignas(8) thread_local u8 nk_buf[PAGE_SIZE];

    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, klen);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        WriteGuard lg(node, v);

        bool found;
        u16 pos = lg->lower_bound(key, klen, found);

        const u8* old_p = nullptr;
        u16 old_l = 0;
        if (found) {
          old_p = lg->get_payload(pos);
          old_l = lg->slots[pos].payload_len;
        }

        u16 new_l = 0;
        if (!mod(old_p, old_l, nk_buf, new_l))
          return false;

        const u8* pk = nullptr; u16 pkl = 0; const u8* pp = nullptr; u16 ppl = 0;
        const u8* sk = nullptr; u16 skl = 0; const u8* sp = nullptr; u16 spl = 0;

        if (found) {
          if (pos > 0) {
            pk = lg->get_key(pos - 1);     pkl = lg->slots[pos - 1].key_len;
            pp = lg->get_payload(pos - 1);  ppl = lg->slots[pos - 1].payload_len;
          } else if (BTreeNode* prev = lg.node()->prev_leaf) {
            u64 pv = prev->lock.read_lock_or_restart();
            if (prev->count > 0) {
              u16 last = prev->count - 1;
              pk = prev->get_key(last);       pkl = prev->slots[last].key_len;
              pp = prev->get_payload(last);   ppl = prev->slots[last].payload_len;
            }
            prev->lock.check_or_restart(pv);
          }
          if (pos + 1 < lg->count) {
            sk = lg->get_key(pos + 1);     skl = lg->slots[pos + 1].key_len;
            sp = lg->get_payload(pos + 1);  spl = lg->slots[pos + 1].payload_len;
          } else if (BTreeNode* next = lg.node()->next_leaf) {
            u64 nv = next->lock.read_lock_or_restart();
            if (next->count > 0) {
              sk = next->get_key(0);       skl = next->slots[0].key_len;
              sp = next->get_payload(0);   spl = next->slots[0].payload_len;
            }
            next->lock.check_or_restart(nv);
          }
        } else {
          if (pos > 0) {
            pk = lg->get_key(pos - 1);     pkl = lg->slots[pos - 1].key_len;
            pp = lg->get_payload(pos - 1);  ppl = lg->slots[pos - 1].payload_len;
          } else if (BTreeNode* prev = lg.node()->prev_leaf) {
            u64 pv = prev->lock.read_lock_or_restart();
            if (prev->count > 0) {
              u16 last = prev->count - 1;
              pk = prev->get_key(last);       pkl = prev->slots[last].key_len;
              pp = prev->get_payload(last);   ppl = prev->slots[last].payload_len;
            }
            prev->lock.check_or_restart(pv);
          }
          if (pos < lg->count) {
            sk = lg->get_key(pos);         skl = lg->slots[pos].key_len;
            sp = lg->get_payload(pos);      spl = lg->slots[pos].payload_len;
          } else if (BTreeNode* next = lg.node()->next_leaf) {
            u64 nv = next->lock.read_lock_or_restart();
            if (next->count > 0) {
              sk = next->get_key(0);       skl = next->slots[0].key_len;
              sp = next->get_payload(0);   spl = next->slots[0].payload_len;
            }
            next->lock.check_or_restart(nv);
          }
        }

        inspector(pk, pkl, pp, ppl, sk, skl, sp, spl);

        if (found) {
          if (new_l == old_l) {
            std::memcpy(lg->get_payload(pos), nk_buf, new_l);
          } else {
            lg->remove_slot(pos);
            if (!lg->has_space_for(klen, new_l)) {
              lg.release();
              return insert(key, klen, nk_buf, new_l);
            }
            lg->insert_in_page(key, klen, nk_buf, new_l);
          }
        } else {
          if (!lg->has_space_for(klen, new_l)) {
            lg.release();
            return insert(key, klen, nk_buf, new_l);
          }
          lg->insert_in_page(key, klen, nk_buf, new_l);
          _size.fetch_add(1);
        }

        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool find_successor(const u8* key, u16 klen, std::function<void(u64)> key_cb,
                      std::function<void(const u8*, u16)> payload_cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, klen);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, klen, found);
        u16 sp = found ? pos + 1 : pos;
        if (sp < node->count) {
          key_cb(0);
          payload_cb(node->get_payload(sp), node->slots[sp].payload_len);
          node->lock.check_or_restart(v);
          return true;
        }
        // Cross-leaf case: follow next_leaf sibling pointer.
        // Safe because merges are disabled -- leaf nodes are never freed during
        // operation, so next_leaf always points to a valid node.
        BTreeNode* next = node->next_leaf;
        node->lock.check_or_restart(v);  // validates next_leaf is consistent
        if (!next) return false;
        u64 nv = next->lock.read_lock_or_restart();
        if (next->count == 0) { next->lock.check_or_restart(nv); return false; }
        key_cb(0);
        payload_cb(next->get_payload(0), next->slots[0].payload_len);
        next->lock.check_or_restart(nv);
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool find_predecessor(const u8* key, u16 klen,
                        std::function<void(const u8*, u16, const u8*, u16)> cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, klen);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, klen, found);
        if (pos > 0) {
          // Common case: predecessor in same leaf.
          cb(node->get_key(pos - 1), node->slots[pos - 1].key_len,
             node->get_payload(pos - 1), node->slots[pos - 1].payload_len);
          node->lock.check_or_restart(v);
          return true;
        }
        // Cross-leaf case: follow prev_leaf sibling pointer.
        // Safe because merges are disabled -- leaf nodes are never freed during
        // operation, so prev_leaf always points to a valid node.
        BTreeNode* prev = node->prev_leaf;
        node->lock.check_or_restart(v);
        if (!prev) return false;
        u64 pv = prev->lock.read_lock_or_restart();
        if (prev->count == 0) { prev->lock.check_or_restart(pv); return false; }
        const u16 last = prev->count - 1;
        cb(prev->get_key(last), prev->slots[last].key_len,
           prev->get_payload(last), prev->slots[last].payload_len);
        prev->lock.check_or_restart(pv);
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  // Non-template methods -- defined in .cpp
  bool insert(const u8* key, u16 klen, const u8* payload, u16 plen);
  bool remove(const u8* key, u16 klen);

  size_t size() const {
    return _size.load();
  }

 private:
  std::atomic<BTreeNode*> _root;
  std::atomic<size_t> _size;

  void try_split(WriteGuard&& leaf, WriteGuard&& parent, const u8* key, u16 klen, u16 plen);
  void ensure_space(BTreeNode* node, const u8* key, u16 klen, u16 plen);
};

enum class DependencyType { FD, OD };

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
    // Coalesce if same CID already present.
    for (uint32_t i = 0; i < _size; ++i) {
      if (_buf[i].commit_id == cid) {
        _buf[i].delta += delta;
        return;
      }
    }
    // Append new entry; silently drop if buffer is full.
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

class DVTransactionContext;

class DependencyValidatingBTree {
 public:
  explicit DependencyValidatingBTree(DependencyType t) : _dep(t) {}

  // Safe unaligned reads (required on ARM64)
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

  MetadataDeltas insert(uint64_t det, uint64_t dep,
                        CommitID commit_cid = CommitID{0},
                        [[maybe_unused]] std::optional<CommitID> lowest_active = std::nullopt) {
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
      return d;  // refcount bumped: tuple tracked, no GVC change
    d.local_delta = new_lv - old_lv;
    d.flag_delta = od_delta;
    _g_history.update(commit_cid, d.total());
    return d;
  }

  MetadataDeltas remove(uint64_t det, uint64_t dep, bool* removed = nullptr,
                        CommitID commit_cid = CommitID{0},
                        [[maybe_unused]] std::optional<CommitID> lowest_active = std::nullopt) {
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
        new_lv = old_lv;
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
      return d;  // refcount decremented: tuple untracked, no GVC change
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

  int global_violation_count(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid);
  }

  int global_violation_count() const {
    return _g_history.query_latest();
  }

  bool dependency_holds(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid) == 0;
  }

  bool dependency_holds() const {
    return _g_history.query_latest() == 0;
  }

  size_t size() const {
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
      return false;
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

  // Recomputes neighbor_flag for det by comparing max_rhs against the successor's
  // min_rhs.  Concurrent correctness relies on the _mod set: modifying succ(k)
  // always adds k to _mod, so _recompute_nflag(k) overwrites any stale value.
  int _recompute_nflag(uint64_t det) {
    u8 lk[8];
    to_key(det, lk);
    uint64_t my_max = 0;
    int old_nf = 0;
    int delta = 0;
    u8* new_hdr_ptr = nullptr;

    _tree.update_with_neighbor_keys(lk, 8,
      [&](const u8* op, u16 ol, u8* np, u16& nl) -> bool {
        if (!op || ol < BVH_SIZE) return false;
        auto h = read_hdr(op);
        my_max = h.max_rhs;
        old_nf = static_cast<int>(h.neighbor_flag);
        std::memcpy(np, op, ol);
        nl = ol;
        new_hdr_ptr = np;
        return true;
      },
      [&](const u8*, u16, const u8*, u16,
          const u8*, u16, const u8* sp, u16 spl) {
        uint64_t succ_min = UINT64_MAX;
        if (sp && spl >= BVH_SIZE)
          succ_min = read_hdr(sp).min_rhs;
        int new_nf = (succ_min != UINT64_MAX && my_max > succ_min) ? 1 : 0;
        delta = new_nf - old_nf;
        if (new_hdr_ptr)
          reinterpret_cast<BTreeValueHeader*>(new_hdr_ptr)->neighbor_flag = uint32_t(new_nf);
      });

    return delta;
  }
};

class DVTransactionContext {
 public:
  explicit DVTransactionContext(DependencyValidatingBTree& t) : _t(t) {}

  DVTransactionContext(const DVTransactionContext&) = delete;
  DVTransactionContext& operator=(const DVTransactionContext&) = delete;

  void insert(uint64_t det, uint64_t dep) {
    const auto key = PendingKey{det, dep};
    auto& delta = _pending[key];
    ++delta;
    if (delta == 0) {
      _pending.erase(key);
    }
  }

  void remove(uint64_t det, uint64_t dep) {
    const auto key = PendingKey{det, dep};
    auto& delta = _pending[key];
    --delta;
    if (delta == 0) {
      _pending.erase(key);
    }
  }

  bool commit(CommitID commit_cid = CommitID{0},
              [[maybe_unused]] std::optional<CommitID> lowest_active = std::nullopt) {
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
      for (uint64_t lhs : _mod) {
        tot += _t._recompute_nflag(lhs);
      }
    }
    _t._g_history.update(commit_cid, tot);
    _pending.clear();
    _mod.clear();
    _ld = 0;
    return true;
  }

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

}  // namespace olc_detail

using DependencyType = olc_detail::DependencyType;

class BTreeOLCIndex : public AbstractChunkIndex {
 public:
  struct MultiValidationState;

  BTreeOLCIndex() = delete;
  BTreeOLCIndex(const BTreeOLCIndex&) = delete;
  BTreeOLCIndex& operator=(const BTreeOLCIndex&) = delete;

  /**
   * Build a chunk index from the given segments.
   * One single-column segment is the common case; multi-column is supported by
   * treating the sequence as a composite key.
   */
  explicit BTreeOLCIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index);

  ~BTreeOLCIndex() override;

  // Call after construction to register (LHS, RHS) pairs for FD/OD tracking.
  void insert_entry_for_validation(const std::vector<AllTypeVariant>& lhs_values,
                                   const std::vector<AllTypeVariant>& rhs_values, DependencyType dep_type,
                                   CommitID commit_cid = CommitID{0},
                                   std::optional<CommitID> lowest_active = std::nullopt);
  void delete_entry_for_validation(const std::vector<AllTypeVariant>& lhs_values,
                                   const std::vector<AllTypeVariant>& rhs_values, DependencyType dep_type,
                                   CommitID commit_cid = CommitID{0},
                                   std::optional<CommitID> lowest_active = std::nullopt);

  [[nodiscard]] int global_violation_count() const;
  [[nodiscard]] int global_violation_count(CommitID snapshot_cid) const;
  [[nodiscard]] bool dependency_holds() const;
  [[nodiscard]] bool dependency_holds(CommitID snapshot_cid) const;

  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

 protected:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const override;
  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const override;
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const override;
  size_t _memory_consumption() const override;

 private:
  // Converts an AllTypeVariant to a uint64_t key usable in DependencyValidatingBTree.
  // Order-preserving for signed integers; hash for strings (OD not meaningful on strings).
  static uint64_t _variant_to_u64(const AllTypeVariant& v);
  static std::vector<uint64_t> _variants_to_u64_tuple(const std::vector<AllTypeVariant>& values);

  // Ensures that dependency type is initialized and remains consistent.
  void _ensure_val_tree(DependencyType dep_type);

  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;

  // Sorted arrays for AbstractChunkIndex iteration (built at construction)
  std::vector<ChunkOffset> _all_offsets;           // positions in sorted-value order
  std::vector<AllTypeVariant> _all_sorted_values;  // parallel to _all_offsets

  // Optional validation tree -- created on first call to insert_entry_for_validation
  std::unique_ptr<olc_detail::DependencyValidatingBTree> _val_tree;
  std::unique_ptr<MultiValidationState> _multi_val_state;
  bool _val_tree_dep_type_initialized = false;
  DependencyType _val_tree_dep_type = DependencyType::FD;

  // First-touch guards for lazy validation-state initialization.
  mutable std::once_flag _validation_dep_type_once;
  mutable std::once_flag _val_tree_init_once;
  mutable std::once_flag _multi_val_state_init_once;
};

}  // namespace hyrise