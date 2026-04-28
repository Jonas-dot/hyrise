#pragma once

// OLC B-Tree ported from btree24 (Müller et al., SIGMOD 2025).
// Source: https://github.com/m-mueller678/btree24/tree/sigmod25
// Locking: per-node version counters (Optimistic Lock Coupling, Leis et al. 2019).

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <random>
#include <thread>

#ifdef __x86_64__
#include <immintrin.h>
#endif

namespace btree_olc {

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;

// PAGE_SIZE is kept only as an upper-bound for TLS key/payload scratch buffers.
// It is NOT used for struct sizing or alignment -- BTreeNode is plain (no alignas).
static constexpr size_t PAGE_SIZE = 4096;

// OLC Exception & Yield
struct OLCRestartException : public std::exception {
  const char* what() const noexcept override {
    return "OLC restart";
  }
};

inline void olc_yield(u64 counter = 0) {
#ifdef __x86_64__
  _mm_pause();
#else
  std::this_thread::yield();
#endif
  if (counter > 5) {
    // randomized backoff to break synchrony between retrying threads
    thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<u64> dist(0, std::min(counter * 5, (u64)500));
    std::this_thread::sleep_for(std::chrono::microseconds(dist(gen)));
  }
}

// Optimistic Lock (from vmache.hpp)
class OptimisticLock {
 public:
  static constexpr u64 UNLOCKED = 0;
  static constexpr u64 LOCKED = 253;

  OptimisticLock() : _state(0) {}

  OptimisticLock(const OptimisticLock&) = delete;
  OptimisticLock& operator=(const OptimisticLock&) = delete;

  u64 read_lock_or_restart() const {
    u64 v = _state.load(std::memory_order_acquire);
    if ((v >> 56) == LOCKED)
      throw OLCRestartException();
    return v;
  }

  // Standard OLC validation (Leis et al. 2019): exact comparison.
  // The << 8 variant was WRONG: it shifted the lock byte out of both sides,
  // meaning "LOCKED at version N" looked identical to "UNLOCKED at version N".
  // A reader that saved version V before a write_lock would see validate(V)==true
  // while the writer was still modifying the node → garbage pointer → SIGBUS.
  bool validate(u64 version) const {
    std::atomic_thread_fence(std::memory_order_acquire);
    return _state.load(std::memory_order_relaxed) == version;
  }

  void check_or_restart(u64 version) const {
    if (!validate(version))
      throw OLCRestartException();
  }

  void write_lock() {
    for (u64 c = 0;; ++c) {
      u64 v = _state.load(std::memory_order_relaxed);
      if ((v >> 56) == UNLOCKED) {
        u64 locked = ((v << 8) >> 8) | (LOCKED << 56);
        if (_state.compare_exchange_weak(v, locked, std::memory_order_acquire))
          return;
      }
      olc_yield(c);
    }
  }

  bool try_write_lock(u64 expected) {
    if ((expected >> 56) != UNLOCKED)
      return false;
    u64 locked = ((expected << 8) >> 8) | (LOCKED << 56);
    return _state.compare_exchange_strong(expected, locked, std::memory_order_acquire);
  }

  void write_unlock() {
    u64 v = _state.load(std::memory_order_relaxed);
    u64 next = (((v << 8) >> 8) + 1) | (UNLOCKED << 56);
    _state.store(next, std::memory_order_release);
  }

 private:
  std::atomic<u64> _state;
};

// Forward declarations
struct BTreeNode;

// Write Guard (RAII)
class WriteGuard {
 public:
  WriteGuard() : _node(nullptr), _lock(nullptr) {}

  explicit WriteGuard(BTreeNode* node);
  WriteGuard(BTreeNode* node, u64 expected_version);

  WriteGuard(WriteGuard&& other) noexcept : _node(other._node), _lock(other._lock) {
    other._node = nullptr;
    other._lock = nullptr;
  }

  WriteGuard& operator=(WriteGuard&& other) noexcept {
    release();
    _node = other._node;
    _lock = other._lock;
    other._node = nullptr;
    other._lock = nullptr;
    return *this;
  }

  ~WriteGuard() {
    release();
  }

  void release();

  BTreeNode* node() const {
    return _node;
  }

  BTreeNode* operator->() const {
    return _node;
  }

 private:
  BTreeNode* _node;
  OptimisticLock* _lock;
};

// BTree Node (from vmcache_btree.hpp)
struct BTreeNode {
  static constexpr u16 HINT_COUNT = 16;
  // UNDERFULL_SIZE is used only by merge_nodes (merges currently disabled).
  // Set to a reasonable fraction; exact value does not affect correctness.
  static constexpr unsigned UNDERFULL_SIZE = 3072;

  OptimisticLock lock;
  bool is_leaf;
  u8 _pad[3];
  u16 count;
  u16 space_used;
  u16 data_offset;
  u16 prefix_len;

  struct FenceSlot {
    u16 offset;
    u16 len;
  };

  FenceSlot lower_fence;
  FenceSlot upper_fence;

  u32 hints[HINT_COUNT];

  union {
    BTreeNode* upper_child;
    BTreeNode* next_leaf;
  };

  BTreeNode* prev_leaf;  // backward sibling pointer (leaf nodes only; null for inner)

  struct Slot {
    u16 offset;
    u16 key_len;
    u16 payload_len;
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

  u8* get_key(u16 slot_id) {
    return ptr() + slots[slot_id].offset;
  }

  const u8* get_key(u16 slot_id) const {
    return ptr() + slots[slot_id].offset;
  }

  u8* get_payload(u16 slot_id) {
    return get_key(slot_id) + slots[slot_id].key_len;
  }

  const u8* get_payload(u16 slot_id) const {
    return get_key(slot_id) + slots[slot_id].key_len;
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
    return data_offset - (reinterpret_cast<const u8*>(&slots[count]) - ptr());
  }

  u16 free_space_after_compaction() const {
    return sizeof(BTreeNode) - (reinterpret_cast<const u8*>(&slots[count]) - ptr()) - space_used;
  }

  u16 space_needed(u16 key_len, u16 payload_len) const {
    return sizeof(Slot) + (key_len > prefix_len ? key_len - prefix_len : 0) + payload_len;
  }

  bool has_space_for(u16 key_len, u16 payload_len) const {
    return space_needed(key_len, payload_len) <= free_space_after_compaction();
  }

  static u32 compute_head(const u8* key, u16 len) {
    switch (len) {
      case 0:
        return 0;
      case 1:
        return static_cast<u32>(key[0]) << 24;
      case 2:
        return (static_cast<u32>(key[0]) << 24) | (static_cast<u32>(key[1]) << 16);
      case 3:
        return (static_cast<u32>(key[0]) << 24) | (static_cast<u32>(key[1]) << 16) | (static_cast<u32>(key[2]) << 8);
      default:
        return (static_cast<u32>(key[0]) << 24) | (static_cast<u32>(key[1]) << 16) | (static_cast<u32>(key[2]) << 8) |
               static_cast<u32>(key[3]);
    }
  }

  void make_hints() {
    if (count == 0)
      return;
    u16 dist = count / (HINT_COUNT + 1);
    for (u16 i = 0; i < HINT_COUNT; ++i) {
      hints[i] = slots[dist * (i + 1)].head;
    }
  }

  void search_hints(u32 key_head, u16& lo, u16& hi) const {
    if (count > HINT_COUNT * 2) {
      u16 dist = hi / (HINT_COUNT + 1);
      u16 pos = 0;
      for (; pos < HINT_COUNT && hints[pos] < key_head; ++pos) {}
      u16 pos2 = pos;
      for (; pos2 < HINT_COUNT && hints[pos2] == key_head; ++pos2) {}
      lo = pos * dist;
      if (pos2 < HINT_COUNT)
        hi = (pos2 + 1) * dist;
    }
  }

  u16 lower_bound(const u8* key, u16 key_len, bool& found) const {
    found = false;
    if (count == 0)
      return 0;

    int cmp = std::memcmp(key, get_prefix(), std::min(static_cast<u16>(key_len), prefix_len));
    if (cmp < 0)
      return 0;
    if (cmp > 0)
      return count;
    if (key_len < prefix_len)
      return 0;

    const u8* search_key = key + prefix_len;
    u16 search_len = key_len - prefix_len;
    u32 key_head = compute_head(search_key, search_len);

    u16 lo = 0, hi = count;
    search_hints(key_head, lo, hi);

    while (lo < hi) {
      u16 mid = lo + (hi - lo) / 2;
      if (key_head < slots[mid].head) {
        hi = mid;
      } else if (key_head > slots[mid].head) {
        lo = mid + 1;
      } else {
        int c = std::memcmp(search_key, get_key(mid), std::min(search_len, slots[mid].key_len));
        if (c < 0) {
          hi = mid;
        } else if (c > 0) {
          lo = mid + 1;
        } else if (search_len < slots[mid].key_len) {
          hi = mid;
        } else if (search_len > slots[mid].key_len) {
          lo = mid + 1;
        } else {
          found = true;
          return mid;
        }
      }
    }
    return lo;
  }

  u16 lower_bound(const u8* key, u16 key_len) const {
    bool f;
    return lower_bound(key, key_len, f);
  }

  BTreeNode* get_child(u16 slot_id) const {
    BTreeNode* child;
    std::memcpy(&child, get_payload(slot_id), sizeof(child));
    return child;
  }

  BTreeNode* lookup_inner(const u8* key, u16 key_len) const {
    u16 pos = lower_bound(key, key_len);
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

  void set_fences(const u8* lower, u16 lower_len, const u8* upper, u16 upper_len) {
    insert_fence(lower_fence, lower, lower_len);
    insert_fence(upper_fence, upper, upper_len);
    for (prefix_len = 0; prefix_len < std::min(lower_len, upper_len) && lower[prefix_len] == upper[prefix_len];
         ++prefix_len) {}
  }

  void store_key_value(u16 slot_id, const u8* key, u16 key_len, const u8* payload, u16 payload_len) {
    const u8* k = key + prefix_len;
    u16 klen = key_len - prefix_len;
    slots[slot_id].head = compute_head(k, klen);
    slots[slot_id].key_len = klen;
    slots[slot_id].payload_len = payload_len;
    u16 space = klen + payload_len;
    data_offset -= space;
    space_used += space;
    slots[slot_id].offset = data_offset;
    std::memcpy(get_key(slot_id), k, klen);
    std::memcpy(get_payload(slot_id), payload, payload_len);
  }

  void insert_in_page(const u8* key, u16 key_len, const u8* payload, u16 payload_len) {
    u16 needed = space_needed(key_len, payload_len);
    if (needed > free_space()) {
      assert(needed <= free_space_after_compaction());
      compactify();
    }
    bool found;
    u16 slot_id = lower_bound(key, key_len, found);
    if (found) {
      space_used -= slots[slot_id].payload_len + slots[slot_id].key_len;
    } else {
      std::memmove(&slots[slot_id + 1], &slots[slot_id], sizeof(Slot) * (count - slot_id));
      count++;
    }
    store_key_value(slot_id, key, key_len, payload, payload_len);
    if (!found)
      make_hints();
  }

  bool remove_slot(u16 slot_id) {
    space_used -= slots[slot_id].key_len + slots[slot_id].payload_len;
    std::memmove(&slots[slot_id], &slots[slot_id + 1], sizeof(Slot) * (count - slot_id - 1));
    count--;
    make_hints();
    return true;
  }

  struct SeparatorInfo {
    u16 length;
    u16 slot;
    bool truncated;
  };

  u16 common_prefix(u16 a, u16 b) const {
    u16 limit = std::min(slots[a].key_len, slots[b].key_len);
    const u8 *ka = get_key(a), *kb = get_key(b);
    u16 i = 0;
    for (; i < limit && ka[i] == kb[i]; ++i) {}
    return i;
  }

  SeparatorInfo find_separator() const {
    assert(count > 1);
    u16 slot = count / 2;
    // Inner nodes: always use full key length (no truncation).
    // Matching btree24: truncation is only safe for leaf nodes.
    if (!is_leaf) {
      return {static_cast<u16>(prefix_len + slots[slot].key_len), slot, false};
    }
    if (slot + 1 < count) {
      u16 common = common_prefix(slot, slot + 1);
      if (slots[slot].key_len > common && slots[slot + 1].key_len > common + 1) {
        return {static_cast<u16>(prefix_len + common + 1), slot, true};
      }
    }
    return {static_cast<u16>(prefix_len + slots[slot].key_len), slot, false};
  }

  void get_separator(u8* out, const SeparatorInfo& info) const {
    std::memcpy(out, get_prefix(), prefix_len);
    std::memcpy(out + prefix_len, get_key(info.slot + (info.truncated ? 1 : 0)), info.length - prefix_len);
  }

  void copy_key_value_range(BTreeNode* dst, u16 dst_slot, u16 src_slot, u16 src_count);
  void split_node(BTreeNode* parent, u16 sep_slot, const u8* sep, u16 sep_len);
  bool merge_nodes(u16 slot_id, BTreeNode* parent, BTreeNode* right);

  static BTreeNode* alloc(bool is_leaf) {
    void* mem = nullptr;
#if defined(__APPLE__) || defined(_POSIX_VERSION)
    if (posix_memalign(&mem, 64, sizeof(BTreeNode)) != 0) {
      throw std::bad_alloc();
    }
#else
    mem = std::aligned_alloc(64, sizeof(BTreeNode));
    if (!mem)
      throw std::bad_alloc();
#endif
    return new (mem) BTreeNode(is_leaf);
  }

  static void dealloc(BTreeNode* node) {
    if (!node) return;
    if (!node->is_leaf) {
      for (u16 i = 0; i < node->count; ++i)
        dealloc(node->get_child(i));
      dealloc(node->upper_child);
    }
    node->~BTreeNode();
    std::free(node);
  }
};

// WriteGuard implementation
inline WriteGuard::WriteGuard(BTreeNode* node) : _node(node), _lock(&node->lock) {
  _lock->write_lock();
}

inline WriteGuard::WriteGuard(BTreeNode* node, u64 expected) : _node(node), _lock(&node->lock) {
  if (!_lock->try_write_lock(expected)) {
    _node = nullptr;
    _lock = nullptr;
    throw OLCRestartException();
  }
}

inline void WriteGuard::release() {
  if (_lock) {
    _lock->write_unlock();
    _lock = nullptr;
    _node = nullptr;
  }
}

// BTree Class
class BTree {
 public:
  BTree() : _root(BTreeNode::alloc(true)), _size(0) {}

  ~BTree() {
    BTreeNode* r = _root.load(std::memory_order_relaxed);
    if (r)
      BTreeNode::dealloc(r);
  }

  template <typename Callback>
  bool lookup(const u8* key, u16 key_len, Callback cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, key_len, found);
        if (found)
          cb(node->get_payload(pos), node->slots[pos].payload_len);
        node->lock.check_or_restart(v);
        return found;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool insert(const u8* key, u16 key_len, const u8* payload, u16 payload_len) {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* parent = nullptr;
        u64 parent_v = 0;
        BTreeNode* node = _root.load(std::memory_order_acquire);

        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          parent = node;
          parent_v = v;
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();

        if (node->has_space_for(key_len, payload_len)) {
          WriteGuard leaf_guard(node, v);
          node->insert_in_page(key, key_len, payload, payload_len);
          _size.fetch_add(1);
          return true;
        }

        if (node->count <= 1)
          throw OLCRestartException();

        WriteGuard parent_guard;
        if (parent)
          parent_guard = WriteGuard(parent, parent_v);
        WriteGuard leaf_guard(node, v);

        try_split(std::move(leaf_guard), std::move(parent_guard), key, key_len, payload_len);
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool remove(const u8* key, u16 key_len) {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);

        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          u16 pos = node->lower_bound(key, key_len);
          BTreeNode* child = (pos == node->count) ? node->upper_child : node->get_child(pos);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, key_len, found);
        if (!found) {
          node->lock.check_or_restart(v);
          return false;
        }

        // No merge: leaf nodes are never freed during operation, which is required
        // for safe OLC traversal in find_successor and find_predecessor without
        // epoch-based memory reclamation. Space is not reclaimed, which is
        // intentional for the dead-row accumulation benchmark.
        WriteGuard leaf_guard(node, v);
        leaf_guard->remove_slot(pos);

        _size.fetch_sub(1);
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  template <typename Modifier>
  bool update_or_insert(const u8* key, u16 key_len, Modifier modifier) {
    alignas(8) thread_local u8 new_payload_buf[PAGE_SIZE];

    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        WriteGuard leaf_guard(node, v);

        bool found;
        u16 pos = leaf_guard->lower_bound(key, key_len, found);

        const u8* old_payload = nullptr;
        u16 old_len = 0;
        if (found) {
          old_payload = leaf_guard->get_payload(pos);
          old_len = leaf_guard->slots[pos].payload_len;
        }

        u16 new_len = 0;
        bool proceed = modifier(old_payload, old_len, new_payload_buf, new_len);
        if (!proceed)
          return false;

        if (found) {
          if (new_len == old_len) {
            std::memcpy(leaf_guard->get_payload(pos), new_payload_buf, new_len);
          } else {
            leaf_guard->remove_slot(pos);
            if (!leaf_guard->has_space_for(key_len, new_len)) {
              leaf_guard.release();
              return insert(key, key_len, new_payload_buf, new_len);
            }
            leaf_guard->insert_in_page(key, key_len, new_payload_buf, new_len);
          }
        } else {
          if (!leaf_guard->has_space_for(key_len, new_len)) {
            leaf_guard.release();
            return insert(key, key_len, new_payload_buf, new_len);
          }
          leaf_guard->insert_in_page(key, key_len, new_payload_buf, new_len);
          _size.fetch_add(1);
        }

        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  size_t size() const {
    return _size.load();
  }

  template <typename Modifier, typename NeighborInspector>
  bool update_with_neighbors(const u8* key, u16 key_len, Modifier modifier, NeighborInspector neighbor_inspector) {
    alignas(8) thread_local u8 new_payload_buf[PAGE_SIZE];

    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        WriteGuard leaf_guard(node, v);

        bool found;
        u16 pos = leaf_guard->lower_bound(key, key_len, found);

        const u8* old_payload = nullptr;
        u16 old_len = 0;
        if (found) {
          old_payload = leaf_guard->get_payload(pos);
          old_len = leaf_guard->slots[pos].payload_len;
        }

        u16 new_len = 0;
        bool proceed = modifier(old_payload, old_len, new_payload_buf, new_len);
        if (!proceed)
          return false;

        const u8* pred_payload = nullptr;
        u16 pred_len = 0;
        const u8* succ_payload = nullptr;
        u16 succ_len = 0;

        if (found) {
          if (pos > 0) {
            pred_payload = leaf_guard->get_payload(pos - 1);
            pred_len = leaf_guard->slots[pos - 1].payload_len;
          }
          if (pos + 1 < leaf_guard->count) {
            succ_payload = leaf_guard->get_payload(pos + 1);
            succ_len = leaf_guard->slots[pos + 1].payload_len;
          }
        } else {
          if (pos > 0) {
            pred_payload = leaf_guard->get_payload(pos - 1);
            pred_len = leaf_guard->slots[pos - 1].payload_len;
          }
          if (pos < leaf_guard->count) {
            succ_payload = leaf_guard->get_payload(pos);
            succ_len = leaf_guard->slots[pos].payload_len;
          }
        }

        neighbor_inspector(pred_payload, pred_len, succ_payload, succ_len);

        if (found) {
          if (new_len == old_len) {
            std::memcpy(leaf_guard->get_payload(pos), new_payload_buf, new_len);
          } else {
            leaf_guard->remove_slot(pos);
            if (!leaf_guard->has_space_for(key_len, new_len)) {
              leaf_guard.release();
              return insert(key, key_len, new_payload_buf, new_len);
            }
            leaf_guard->insert_in_page(key, key_len, new_payload_buf, new_len);
          }
        } else {
          if (!leaf_guard->has_space_for(key_len, new_len)) {
            leaf_guard.release();
            return insert(key, key_len, new_payload_buf, new_len);
          }
          leaf_guard->insert_in_page(key, key_len, new_payload_buf, new_len);
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
  template <typename Modifier, typename NeighborKeyInspector>
  bool update_with_neighbor_keys(const u8* key, u16 key_len, Modifier modifier, NeighborKeyInspector inspector) {
    alignas(8) thread_local u8 nk_payload_buf[PAGE_SIZE];

    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        WriteGuard lg(node, v);

        bool found;
        u16 pos = lg->lower_bound(key, key_len, found);

        const u8* old_payload = nullptr;
        u16 old_len = 0;
        if (found) {
          old_payload = lg->get_payload(pos);
          old_len = lg->slots[pos].payload_len;
        }

        u16 new_len = 0;
        bool proceed = modifier(old_payload, old_len, nk_payload_buf, new_len);
        if (!proceed)
          return false;

        const u8* pk = nullptr; u16 pkl = 0; const u8* pp = nullptr; u16 ppl = 0;
        const u8* sk = nullptr; u16 skl = 0; const u8* sp = nullptr; u16 spl = 0;

        auto read_pred = [&](u16 p) {
          pk = lg->get_key(p);       pkl = lg->slots[p].key_len;
          pp = lg->get_payload(p);   ppl = lg->slots[p].payload_len;
        };
        auto read_succ = [&](u16 p) {
          sk = lg->get_key(p);       skl = lg->slots[p].key_len;
          sp = lg->get_payload(p);   spl = lg->slots[p].payload_len;
        };

        if (found) {
          if (pos > 0) {
            read_pred(pos - 1);
          } else if (BTreeNode* prev = lg.node()->prev_leaf) {
            // Cross-leaf predecessor: follow sibling pointer (safe -- merges are disabled).
            u64 pv = prev->lock.read_lock_or_restart();
            if (prev->count > 0) {
              u16 last = prev->count - 1;
              pk = prev->get_key(last);       pkl = prev->slots[last].key_len;
              pp = prev->get_payload(last);   ppl = prev->slots[last].payload_len;
            }
            prev->lock.check_or_restart(pv);
          }
          if (pos + 1 < lg->count) {
            read_succ(pos + 1);
          } else if (BTreeNode* next = lg.node()->next_leaf) {
            // Cross-leaf successor: follow sibling pointer.
            u64 nv = next->lock.read_lock_or_restart();
            if (next->count > 0) {
              sk = next->get_key(0);       skl = next->slots[0].key_len;
              sp = next->get_payload(0);   spl = next->slots[0].payload_len;
            }
            next->lock.check_or_restart(nv);
          }
        } else {
          if (pos > 0) {
            read_pred(pos - 1);
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
            read_succ(pos);
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
          if (new_len == old_len) {
            std::memcpy(lg->get_payload(pos), nk_payload_buf, new_len);
          } else {
            lg->remove_slot(pos);
            if (!lg->has_space_for(key_len, new_len)) {
              lg.release();
              return insert(key, key_len, nk_payload_buf, new_len);
            }
            lg->insert_in_page(key, key_len, nk_payload_buf, new_len);
          }
        } else {
          if (!lg->has_space_for(key_len, new_len)) {
            lg.release();
            return insert(key, key_len, nk_payload_buf, new_len);
          }
          lg->insert_in_page(key, key_len, nk_payload_buf, new_len);
          _size.fetch_add(1);
        }

        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool find_neighbors(const u8* key, u16 key_len, std::function<void(const u8*, u16)> pred_cb,
                      std::function<void(const u8*, u16)> succ_cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        u16 pos = node->lower_bound(key, key_len);
        if (pos > 0)
          pred_cb(node->get_payload(pos - 1), node->slots[pos - 1].payload_len);
        else
          pred_cb(nullptr, 0);

        bool found;
        u16 exact_pos = node->lower_bound(key, key_len, found);
        u16 succ_pos = found ? exact_pos + 1 : exact_pos;
        if (succ_pos < node->count)
          succ_cb(node->get_payload(succ_pos), node->slots[succ_pos].payload_len);
        else
          succ_cb(nullptr, 0);

        node->lock.check_or_restart(v);
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool find_successor(const u8* key, u16 key_len, std::function<void(u64)> succ_key_cb,
                      std::function<void(const u8*, u16)> succ_payload_cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }
        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, key_len, found);
        u16 succ_pos = found ? pos + 1 : pos;
        if (succ_pos < node->count) {
          // Common case: successor in same leaf.
          succ_key_cb(0);
          succ_payload_cb(node->get_payload(succ_pos), node->slots[succ_pos].payload_len);
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
        succ_key_cb(0);
        succ_payload_cb(next->get_payload(0), next->slots[0].payload_len);
        next->lock.check_or_restart(nv);
        return true;
      } catch (const OLCRestartException&) {
        olc_yield(retry);
      }
    }
  }

  bool find_predecessor(const u8* key, u16 key_len,
                        std::function<void(const u8*, u16, const u8*, u16)> cb) const {
    for (u64 retry = 0;; ++retry) {
      try {
        BTreeNode* node = _root.load(std::memory_order_acquire);
        while (!node->is_leaf) {
          u64 v = node->lock.read_lock_or_restart();
          BTreeNode* child = node->lookup_inner(key, key_len);
          node->lock.check_or_restart(v);
          node = child;
        }

        u64 v = node->lock.read_lock_or_restart();
        bool found;
        u16 pos = node->lower_bound(key, key_len, found);
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
        node->lock.check_or_restart(v);  // validates prev_leaf is consistent
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

 private:
  std::atomic<BTreeNode*> _root;
  std::atomic<size_t> _size;

  void try_split(WriteGuard&& leaf, WriteGuard&& parent, const u8* key, u16 key_len, u16 payload_len);
  void ensure_space(BTreeNode* to_split, const u8* key, u16 key_len, u16 payload_len);
};

}  // namespace btree_olc