#pragma once

// OLC B-Tree integrated as a Hyrise AbstractChunkIndex.
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
#include <memory>
#include <random>
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

}  // namespace olc_detail

class BTreeOLCIndex : public AbstractChunkIndex {
 public:
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

  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

 protected:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const override;
  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const override;
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const override;
  size_t _memory_consumption() const override;

 private:
  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;

  // Sorted arrays for AbstractChunkIndex iteration (built at construction)
  std::vector<ChunkOffset> _all_offsets;           // positions in sorted-value order
  std::vector<AllTypeVariant> _all_sorted_values;  // parallel to _all_offsets
};

}  // namespace hyrise
