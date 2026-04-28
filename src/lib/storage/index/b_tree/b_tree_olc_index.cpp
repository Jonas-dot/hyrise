#include "b_tree_olc_index.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#include "storage/abstract_segment.hpp"

namespace hyrise {
namespace olc_detail {

thread_local u8 tls_btree_sep_buf[PAGE_SIZE];
static thread_local u8 tls_key_buf[PAGE_SIZE];  // for copy_key_value_range

void BTreeNode::compactify() {
  BTreeNode tmp(is_leaf);
  tmp.set_fences(get_lower_fence(), lower_fence.len, get_upper_fence(), upper_fence.len);
  copy_key_value_range(&tmp, 0, 0, count);
  tmp.upper_child = upper_child;
  u64 ofs = sizeof(OptimisticLock);
  std::memcpy(ptr() + ofs, tmp.ptr() + ofs, sizeof(BTreeNode) - ofs);
  make_hints();
}

void BTreeNode::copy_key_value_range(BTreeNode* dst, u16 dslot, u16 sslot, u16 scount) {
  if (prefix_len <= dst->prefix_len) {
    u16 diff = dst->prefix_len - prefix_len;
    for (u16 i = 0; i < scount; ++i) {
      u16 nkl = slots[sslot + i].key_len - diff;
      u16 space = nkl + slots[sslot + i].payload_len;
      dst->data_offset -= space;
      dst->space_used += space;
      dst->slots[dslot + i].offset = dst->data_offset;
      const u8* key = get_key(sslot + i) + diff;
      std::memcpy(dst->get_key(dslot + i), key, space);
      dst->slots[dslot + i].head = compute_head(key, nkl);
      dst->slots[dslot + i].key_len = nkl;
      dst->slots[dslot + i].payload_len = slots[sslot + i].payload_len;
    }
  } else {
    for (u16 i = 0; i < scount; ++i) {
      u16 full_len = slots[sslot + i].key_len + prefix_len;
      assert(full_len <= PAGE_SIZE);
      std::memcpy(tls_key_buf, get_lower_fence(), prefix_len);
      std::memcpy(tls_key_buf + prefix_len, get_key(sslot + i), slots[sslot + i].key_len);
      dst->store_key_value(dslot + i, tls_key_buf, full_len, get_payload(sslot + i), slots[sslot + i].payload_len);
    }
  }
  dst->count += scount;
}

void BTreeNode::split_node(BTreeNode* parent, u16 sep_slot, const u8* sep, u16 sep_len) {
  assert(sep_slot > 0);
  BTreeNode tmp(is_leaf);
  BTreeNode* left = &tmp;
  BTreeNode* right = BTreeNode::alloc(is_leaf);

  left->set_fences(get_lower_fence(), lower_fence.len, sep, sep_len);
  right->set_fences(sep, sep_len, get_upper_fence(), upper_fence.len);

  u16 old_slot = parent->lower_bound(sep, sep_len);
  if (old_slot == parent->count) {
    parent->upper_child = right;
  } else {
    std::memcpy(parent->get_payload(old_slot), &right, sizeof(right));
  }
  BTreeNode* self = this;
  parent->insert_in_page(sep, sep_len, reinterpret_cast<u8*>(&self), sizeof(BTreeNode*));

  if (is_leaf) {
    copy_key_value_range(left, 0, 0, sep_slot + 1);
    copy_key_value_range(right, 0, left->count, count - left->count);
    left->next_leaf  = right;
    right->next_leaf = this->next_leaf;
    left->prev_leaf  = this->prev_leaf;  // copied to `this` by the memcpy below
    // right->prev_leaf set to `this` after memcpy -- `left` is a stack-allocated temp
  } else {
    copy_key_value_range(left, 0, 0, sep_slot);
    copy_key_value_range(right, 0, left->count + 1, count - left->count - 1);
    left->upper_child = get_child(left->count);
    right->upper_child = upper_child;
  }

  left->make_hints();
  right->make_hints();
  u64 ofs = sizeof(OptimisticLock);
  std::memcpy(ptr() + ofs, left->ptr() + ofs, sizeof(BTreeNode) - ofs);
  // After memcpy: `this` is the left half. right->prev_leaf must point to `this`,
  // not to `left` (a stack temporary that is now out of scope).
  if (is_leaf) {
    right->prev_leaf = this;
  }
}

bool BTreeNode::merge_nodes(u16 /*slot_id*/, BTreeNode* /*parent*/, BTreeNode* /*right*/) {
  // Merging is intentionally disabled: space reclamation is the DBMS's responsibility,
  // not the index's. Freeing leaf nodes during concurrent OLC traversal requires
  // epoch-based memory reclamation which is not implemented here.
  return false;
}


bool BTree::insert(const u8* key, u16 klen, const u8* payload, u16 plen) {
  for (u64 retry = 0;; ++retry) {
    try {
      BTreeNode* parent = nullptr;
      u64 pv = 0;
      BTreeNode* node = _root.load(std::memory_order_acquire);

      // Traverse to leaf (optimistic reads only, no proactive splitting)
      while (!node->is_leaf) {
        u64 v = node->lock.read_lock_or_restart();
        parent = node;
        pv = v;
        BTreeNode* child = node->lookup_inner(key, klen);
        node->lock.check_or_restart(v);
        node = child;
      }

      u64 v = node->lock.read_lock_or_restart();

      if (node->has_space_for(klen, plen)) {
        // Leaf has space: lock leaf only, insert
        WriteGuard lg(node, v);
        node->insert_in_page(key, klen, payload, plen);
        _size.fetch_add(1);
        return true;
      }

      // Leaf is full -- lock parent and leaf, then split
      if (node->count <= 1)
        throw OLCRestartException();

      WriteGuard pg_guard;
      if (parent)
        pg_guard = WriteGuard(parent, pv);
      WriteGuard lg(node, v);

      try_split(std::move(lg), std::move(pg_guard), key, klen, plen);
      // Split done; insert did not happen -- restart from root
    } catch (const OLCRestartException&) {
      olc_yield(retry);
    }
  }
}

bool BTree::remove(const u8* key, u16 klen) {
  for (u64 retry = 0;; ++retry) {
    try {
      BTreeNode* node = _root.load(std::memory_order_acquire);

      while (!node->is_leaf) {
        u64 v = node->lock.read_lock_or_restart();
        u16 pos = node->lower_bound(key, klen);
        BTreeNode* child = (pos == node->count) ? node->upper_child : node->get_child(pos);
        node->lock.check_or_restart(v);
        node = child;
      }

      u64 v = node->lock.read_lock_or_restart();
      bool found;
      u16 pos = node->lower_bound(key, klen, found);
      if (!found) {
        node->lock.check_or_restart(v);
        return false;
      }

      // No merge: leaf nodes are never freed during operation, which is required
      // for safe OLC traversal in find_successor and find_predecessor without
      // epoch-based memory reclamation.
      WriteGuard lg(node, v);
      lg->remove_slot(pos);

      _size.fetch_sub(1);
      return true;
    } catch (const OLCRestartException&) {
      olc_yield(retry);
    }
  }
}

void BTree::try_split(WriteGuard&& node_guard, WriteGuard&& parent_guard, const u8* key, u16 klen, u16 plen) {
  BTreeNode* node = node_guard.node();
  BTreeNode* parent = parent_guard.node();

  if (!parent) {
    BTreeNode* nr = BTreeNode::alloc(false);
    nr->upper_child = node;
    parent_guard = WriteGuard(nr);  // lock BEFORE publishing (matches btree24)
    _root.store(nr, std::memory_order_release);
    parent = nr;
  }

  if (node->count <= 1)
    return;

  auto si = node->find_separator();
  assert(si.length <= sizeof(BTreeNode));
  u8 sep_key[PAGE_SIZE];
  node->get_separator(sep_key, si);

  if (parent->has_space_for(si.length, sizeof(BTreeNode*))) {
    node->split_node(parent, si.slot, sep_key, si.length);

    // Update old_next->prev_leaf to point to the new right half (leaf splits only).
    if (node->is_leaf) {
      BTreeNode* right    = node->next_leaf;
      BTreeNode* old_next = right->next_leaf;
      if (old_next) {
        WriteGuard og(old_next);
        old_next->prev_leaf = right;
      }
    }
    return;
  }

  // Parent does not have space -- must split parent first.
  BTreeNode* parent_ptr = parent;
  node_guard = WriteGuard();
  parent_guard = WriteGuard();
  ensure_space(parent_ptr, sep_key, si.length, sizeof(BTreeNode*));
}

void BTree::ensure_space(BTreeNode* to_split, const u8* key, u16 klen, u16 plen) {
  for (u64 retry = 0;; ++retry) {
    try {
      BTreeNode* parent = nullptr;
      u64 pv = 0;
      BTreeNode* node = _root.load(std::memory_order_acquire);

      while (!node->is_leaf && node != to_split) {
        u64 v = node->lock.read_lock_or_restart();
        parent = node;
        pv = v;
        BTreeNode* child = node->lookup_inner(key, klen);
        node->lock.check_or_restart(v);
        node = child;
      }

      if (node == to_split) {
        u64 v = node->lock.read_lock_or_restart();
        if (node->has_space_for(klen, plen)) {
          node->lock.check_or_restart(v);
          return;
        }
        WriteGuard pg;
        if (parent)
          pg = WriteGuard(parent, pv);
        WriteGuard ng(node, v);
        try_split(std::move(ng), std::move(pg), key, klen, plen);
      }
      return;
    } catch (const OLCRestartException&) {
      olc_yield(retry);
    }
  }
}

}  // namespace olc_detail

struct BTreeOLCIndex::MultiValidationState {
  using Tuple = std::vector<uint64_t>;
  using u8 = olc_detail::u8;
  using u16 = olc_detail::u16;

  explicit MultiValidationState(DependencyType dep_type) : _dependency_type(dep_type) {}

  bool insert(const Tuple& lhs, const Tuple& rhs,
              CommitID commit_cid = CommitID{0},
              [[maybe_unused]] std::optional<CommitID> lowest_active = std::nullopt) {
    if (lhs.empty() || rhs.empty()) {
      return false;
    }

    const auto lhs_key = _serialize_lhs_key(lhs);
    const auto rhs_tuple = _serialize_rhs_tuple(rhs);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    bool refcount_bump = false;
    int old_local = 0;
    int new_local = 0;
    bool was_new_entry = false;
    TupleSnapshot old_max;
    TupleSnapshot new_min;
    TupleSnapshot new_max;
    int od_boundary_delta = 0;

    auto modifier = [&](const u8* old_payload, u16 old_len, u8* new_payload, u16& new_len) -> bool {
      return _insert_modifier_tuple(rhs_tuple, tuple_width, old_payload, old_len, new_payload, new_len, refcount_bump,
                                    old_local, new_local, was_new_entry, old_max, new_min, new_max);
    };

    if (_dependency_type == DependencyType::FD) {
      _tree.update_or_insert(lhs_key.data(), static_cast<u16>(lhs_key.size()), modifier);
    } else {
      _tree.update_with_neighbors(
          lhs_key.data(), static_cast<u16>(lhs_key.size()), modifier,
          [&](const u8* pred_payload, u16 pred_len, const u8* succ_payload, u16 succ_len) {
            if (refcount_bump) {
              return;
            }

            const u8* pred_max = nullptr;
            if (was_new_entry && _read_max_tuple_ptr(pred_payload, pred_len, tuple_width, pred_max)) {
              if (_tuple_cmp(pred_max, new_min.data(), tuple_width) > 0) {
                od_boundary_delta += 1;
              }
            }

            const u8* succ_min = nullptr;
            if (_read_min_tuple_ptr(succ_payload, succ_len, tuple_width, succ_min)) {
              const auto old_violation =
                  !was_new_entry && !old_max.empty() && (_tuple_cmp(old_max.data(), succ_min, tuple_width) > 0);
              const auto new_violation = !new_max.empty() && (_tuple_cmp(new_max.data(), succ_min, tuple_width) > 0);
              if (new_violation && !old_violation) {
                od_boundary_delta += 1;
              }
              if (!new_violation && old_violation) {
                od_boundary_delta -= 1;
              }
            }
          });
    }

    _total_entries.fetch_add(1, std::memory_order_relaxed);
    if (refcount_bump) {
      return false;
    }
    _g_history.update(commit_cid, (new_local - old_local) + od_boundary_delta);
    return true;
  }

  bool remove(const Tuple& lhs, const Tuple& rhs,
              CommitID commit_cid = CommitID{0},
              [[maybe_unused]] std::optional<CommitID> lowest_active = std::nullopt) {
    if (lhs.empty() || rhs.empty()) {
      return false;
    }

    const auto lhs_key = _serialize_lhs_key(lhs);
    const auto rhs_tuple = _serialize_rhs_tuple(rhs);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    bool was_found = false;
    bool should_remove_entry = false;
    bool refcount_only = false;
    int old_local = 0;
    int new_local = 0;
    TupleSnapshot old_min;
    TupleSnapshot old_max;
    TupleSnapshot new_min;
    TupleSnapshot new_max;
    int od_boundary_delta = 0;

    auto remove_modifier = [&](const u8* old_payload, u16 old_len, u8* new_payload, u16& new_len) -> bool {
      if (!old_payload || old_len < sizeof(TuplePayloadHeader)) {
        return false;
      }

      TuplePayloadHeader hdr;
      std::memcpy(&hdr, old_payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width) {
        return false;
      }

      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * _entry_stride(hdr.tuple_width);
      if (old_len < required_len) {
        return false;
      }

      auto remove_pos = -1;
      const auto* tuples_base = old_payload + sizeof(TuplePayloadHeader);
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const auto* tuple = tuples_base + i * _entry_stride(hdr.tuple_width);
        if (_tuple_cmp(tuple, rhs_tuple.data(), tuple_width) == 0) {
          remove_pos = static_cast<int>(i);
          break;
        }
      }

      if (remove_pos < 0) {
        return false;
      }

      was_found = true;
      old_local = static_cast<int>(hdr.local_violation);

      // Scan for old min/max.
      const u8* min_ptr = tuples_base;
      const u8* max_ptr = tuples_base;
      for (uint32_t i = 1; i < hdr.count; ++i) {
        const auto* cur = tuples_base + i * _entry_stride(hdr.tuple_width);
        if (_tuple_cmp(cur, min_ptr, tuple_width) < 0) {
          min_ptr = cur;
        }
        if (_tuple_cmp(cur, max_ptr, tuple_width) > 0) {
          max_ptr = cur;
        }
      }
      old_min.assign(min_ptr, hdr.tuple_width);
      old_max.assign(max_ptr, hdr.tuple_width);

      // Refcount check: if > 1, decrement only
      if (_read_refcount(tuples_base, static_cast<uint32_t>(remove_pos), tuple_width) > 1) {
        std::memcpy(new_payload, old_payload, old_len);
        const auto old_rc = _read_refcount(new_payload + sizeof(TuplePayloadHeader),
                                           static_cast<uint32_t>(remove_pos), tuple_width);
        _write_refcount(new_payload + sizeof(TuplePayloadHeader), static_cast<uint32_t>(remove_pos), tuple_width,
                        old_rc - 1);
        new_len = old_len;
        refcount_only = true;
        return true;
      }

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

      auto* out_base = new_payload + sizeof(TuplePayloadHeader);
      uint32_t out_idx = 0;
      for (uint32_t i = 0; i < hdr.count; ++i) {
        if (static_cast<int>(i) == remove_pos) {
          continue;
        }
        const auto* src = tuples_base + i * _entry_stride(hdr.tuple_width);
        std::memcpy(out_base + out_idx * _entry_stride(hdr.tuple_width), src, _entry_stride(hdr.tuple_width));
        ++out_idx;
      }

      new_local = static_cast<int>(new_hdr->local_violation);
      // Scan remaining entries for new min/max.
      const u8* nmin = out_base;
      const u8* nmax = out_base;
      for (uint32_t i = 1; i < new_hdr->count; ++i) {
        const auto* cur = out_base + i * _entry_stride(new_hdr->tuple_width);
        if (_tuple_cmp(cur, nmin, tuple_width) < 0) {
          nmin = cur;
        }
        if (_tuple_cmp(cur, nmax, tuple_width) > 0) {
          nmax = cur;
        }
      }
      new_min.assign(nmin, new_hdr->tuple_width);
      new_max.assign(nmax, new_hdr->tuple_width);
      new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + new_hdr->count * _entry_stride(new_hdr->tuple_width));
      return true;
    };

    if (_dependency_type == DependencyType::OD) {
      _tree.update_with_neighbors(
          lhs_key.data(), static_cast<u16>(lhs_key.size()), remove_modifier,
          [&](const u8* pred_payload, u16 pred_len, const u8* succ_payload, u16 succ_len) {
            if (!was_found) {
              return;
            }

            const u8* pred_max = nullptr;
            const u8* succ_min = nullptr;
            const auto has_pred_max = _read_max_tuple_ptr(pred_payload, pred_len, tuple_width, pred_max);
            const auto has_succ_min = _read_min_tuple_ptr(succ_payload, succ_len, tuple_width, succ_min);

            auto old_boundary_violations = 0;
            auto new_boundary_violations = 0;

            if (has_pred_max && !old_min.empty() && _tuple_cmp(pred_max, old_min.data(), tuple_width) > 0) {
              ++old_boundary_violations;
            }

            if (should_remove_entry) {
              if (has_pred_max && has_succ_min && _tuple_cmp(pred_max, succ_min, tuple_width) > 0) {
                ++new_boundary_violations;
              }
            } else {
              if (has_pred_max && !new_min.empty() && _tuple_cmp(pred_max, new_min.data(), tuple_width) > 0) {
                ++new_boundary_violations;
              }
            }

            if (!old_max.empty() && has_succ_min && _tuple_cmp(old_max.data(), succ_min, tuple_width) > 0) {
              ++old_boundary_violations;
            }

            if (!should_remove_entry && !new_max.empty() && has_succ_min &&
                _tuple_cmp(new_max.data(), succ_min, tuple_width) > 0) {
              ++new_boundary_violations;
            }

            od_boundary_delta += (new_boundary_violations - old_boundary_violations);
          });
    } else {
      _tree.update_or_insert(lhs_key.data(), static_cast<u16>(lhs_key.size()), remove_modifier);
    }

    if (!was_found && !should_remove_entry) {
      return false;
    }

    _total_entries.fetch_sub(1, std::memory_order_relaxed);
    if (refcount_only) {
      return true;
    }

    if (should_remove_entry) {
      _g_history.update(commit_cid, (-old_local) + od_boundary_delta);
      _tree.remove(lhs_key.data(), static_cast<u16>(lhs_key.size()));
    } else {
      _g_history.update(commit_cid, (new_local - old_local) + od_boundary_delta);
    }

    return true;
  }

  bool lookup(const Tuple& lhs, const Tuple& rhs) const {
    if (lhs.empty() || rhs.empty()) {
      return false;
    }

    const auto lhs_key = _serialize_lhs_key(lhs);
    const auto rhs_tuple = _serialize_rhs_tuple(rhs);
    const auto tuple_width = static_cast<uint32_t>(rhs_tuple.size());

    auto found = false;
    _tree.lookup(lhs_key.data(), static_cast<u16>(lhs_key.size()), [&](const u8* payload, u16 len) {
      if (!payload || len < sizeof(TuplePayloadHeader)) {
        return;
      }

      TuplePayloadHeader hdr;
      std::memcpy(&hdr, payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width) {
        return;
      }

      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * _entry_stride(hdr.tuple_width);
      if (len < required_len) {
        return;
      }

      const auto* base = payload + sizeof(TuplePayloadHeader);
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const auto* tuple = base + i * _entry_stride(hdr.tuple_width);
        if (_tuple_cmp(tuple, rhs_tuple.data(), tuple_width) == 0) {
          found = true;
          break;
        }
      }
    });

    return found;
  }

  int global_violation_count() const {
    return _g_history.query_latest();
  }

  int global_violation_count(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid);
  }

  bool dependency_holds() const {
    return _g_history.query_latest() == 0;
  }

  bool dependency_holds(CommitID snapshot_cid) const {
    return _g_history.query(snapshot_cid) == 0;
  }

  size_t size() const {
    return _total_entries.load(std::memory_order_relaxed);
  }

  size_t approximate_memory_usage() const {
    return sizeof(*this) + _tree.size() * (sizeof(TuplePayloadHeader) + 64u);
  }

 private:
  struct TuplePayloadHeader {
    uint32_t count;
    uint32_t local_violation;
    uint32_t neighbor_flag;
    uint32_t tuple_width;
  };

  static uint32_t _entry_stride(uint32_t tw) { return tw + sizeof(uint32_t); }

  static uint32_t _read_refcount(const u8* base, uint32_t i, uint32_t tw) {
    uint32_t rc;
    std::memcpy(&rc, base + i * _entry_stride(tw) + tw, sizeof(rc));
    return rc;
  }

  static void _write_refcount(u8* base, uint32_t i, uint32_t tw, uint32_t rc) {
    std::memcpy(base + i * _entry_stride(tw) + tw, &rc, sizeof(rc));
  }

  struct TupleSnapshot {
    std::array<u8, 32> inline_data{};
    std::vector<u8> heap_data;
    uint32_t width = 0;

    void assign(const u8* src, uint32_t tuple_width) {
      width = tuple_width;
      if (tuple_width <= inline_data.size()) {
        std::memcpy(inline_data.data(), src, tuple_width);
        heap_data.clear();
      } else {
        heap_data.assign(src, src + tuple_width);
      }
    }

    [[nodiscard]] const u8* data() const {
      return width <= inline_data.size() ? inline_data.data() : heap_data.data();
    }

    [[nodiscard]] bool empty() const {
      return width == 0;
    }
  };

  static int _tuple_cmp(const u8* a, const u8* b, uint32_t width) {
    return std::memcmp(a, b, width);
  }

  static std::vector<u8> _serialize_lhs_key(const Tuple& lhs) {
    std::vector<u8> out;
    out.resize(2 + lhs.size() * sizeof(uint64_t));
    out[0] = static_cast<u8>((lhs.size() >> 8) & 0xFF);
    out[1] = static_cast<u8>(lhs.size() & 0xFF);

    auto pos = size_t{2};
    for (const auto value : lhs) {
      for (int i = 7; i >= 0; --i) {
        out[pos++] = static_cast<u8>((value >> (i * 8)) & 0xFF);
      }
    }

    return out;
  }

  static std::vector<u8> _serialize_rhs_tuple(const Tuple& rhs) {
    std::vector<u8> out;
    out.resize(rhs.size() * sizeof(uint64_t));

    auto pos = size_t{0};
    for (const auto value : rhs) {
      for (int i = 7; i >= 0; --i) {
        out[pos++] = static_cast<u8>((value >> (i * 8)) & 0xFF);
      }
    }

    return out;
  }

  static bool _read_min_tuple_ptr(const u8* payload, u16 len, uint32_t expected_width, const u8*& out_ptr) {
    out_ptr = nullptr;
    if (!payload || len < sizeof(TuplePayloadHeader)) {
      return false;
    }

    TuplePayloadHeader hdr;
    std::memcpy(&hdr, payload, sizeof(hdr));
    if (hdr.count == 0 || hdr.tuple_width == 0 || hdr.tuple_width != expected_width) {
      return false;
    }

    const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * _entry_stride(hdr.tuple_width);
    if (len < required_len) {
      return false;
    }

    const auto* base = payload + sizeof(TuplePayloadHeader);
    out_ptr = base;
    for (uint32_t i = 1; i < hdr.count; ++i) {
      const auto* cur = base + i * _entry_stride(hdr.tuple_width);
      if (_tuple_cmp(cur, out_ptr, hdr.tuple_width) < 0) {
        out_ptr = cur;
      }
    }
    return true;
  }

  static bool _read_max_tuple_ptr(const u8* payload, u16 len, uint32_t expected_width, const u8*& out_ptr) {
    out_ptr = nullptr;
    if (!payload || len < sizeof(TuplePayloadHeader)) {
      return false;
    }

    TuplePayloadHeader hdr;
    std::memcpy(&hdr, payload, sizeof(hdr));
    if (hdr.count == 0 || hdr.tuple_width == 0 || hdr.tuple_width != expected_width) {
      return false;
    }

    const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * _entry_stride(hdr.tuple_width);
    if (len < required_len) {
      return false;
    }

    const auto* base = payload + sizeof(TuplePayloadHeader);
    out_ptr = base;
    for (uint32_t i = 1; i < hdr.count; ++i) {
      const auto* cur = base + i * _entry_stride(hdr.tuple_width);
      if (_tuple_cmp(cur, out_ptr, hdr.tuple_width) > 0) {
        out_ptr = cur;
      }
    }
    return true;
  }

  static bool _insert_modifier_tuple(const std::vector<u8>& rhs_tuple, uint32_t tuple_width, const u8* old_payload,
                                     u16 old_len, u8* new_payload, u16& new_len, bool& refcount_bump, int& old_local,
                                     int& new_local, bool& was_new_entry, TupleSnapshot& old_max,
                                     TupleSnapshot& new_min, TupleSnapshot& new_max) {
    if (old_payload && old_len >= sizeof(TuplePayloadHeader)) {
      TuplePayloadHeader hdr;
      std::memcpy(&hdr, old_payload, sizeof(hdr));
      if (hdr.tuple_width != tuple_width) {
        return false;
      }

      const auto required_len = sizeof(TuplePayloadHeader) + hdr.count * _entry_stride(hdr.tuple_width);
      if (old_len < required_len) {
        return false;
      }

      old_local = static_cast<int>(hdr.local_violation);
      const auto* old_base = old_payload + sizeof(TuplePayloadHeader);
      const auto stride = _entry_stride(hdr.tuple_width);

      // Scan for old min/max and check for duplicate.
      const u8* min_ptr = old_base;
      const u8* max_ptr = old_base;
      auto dup_pos = -1;
      for (uint32_t i = 0; i < hdr.count; ++i) {
        const auto* cur = old_base + i * stride;
        if (_tuple_cmp(cur, rhs_tuple.data(), tuple_width) == 0) {
          dup_pos = static_cast<int>(i);
        }
        if (_tuple_cmp(cur, min_ptr, tuple_width) < 0) {
          min_ptr = cur;
        }
        if (_tuple_cmp(cur, max_ptr, tuple_width) > 0) {
          max_ptr = cur;
        }
      }
      old_max.assign(max_ptr, hdr.tuple_width);

      if (dup_pos >= 0) {
        // Existing entry: bump its refcount only.
        std::memcpy(new_payload, old_payload, old_len);
        const auto old_rc = _read_refcount(new_payload + sizeof(TuplePayloadHeader), dup_pos, tuple_width);
        _write_refcount(new_payload + sizeof(TuplePayloadHeader), dup_pos, tuple_width, old_rc + 1);
        new_len = old_len;
        refcount_bump = true;
        return true;
      }

      // New distinct value: append at end.
      auto* out_hdr = reinterpret_cast<TuplePayloadHeader*>(new_payload);
      out_hdr->count = hdr.count + 1;
      out_hdr->tuple_width = hdr.tuple_width;
      out_hdr->neighbor_flag = hdr.neighbor_flag;
      out_hdr->local_violation = out_hdr->count > 0 ? out_hdr->count - 1 : 0;

      auto* out_base = new_payload + sizeof(TuplePayloadHeader);
      std::memcpy(out_base, old_base, hdr.count * stride);
      std::memcpy(out_base + hdr.count * stride, rhs_tuple.data(), hdr.tuple_width);
      const uint32_t rc_one = 1;
      std::memcpy(out_base + hdr.count * stride + hdr.tuple_width, &rc_one, sizeof(rc_one));

      new_local = static_cast<int>(out_hdr->local_violation);
      // new min/max = min/max(old, inserted)
      if (_tuple_cmp(rhs_tuple.data(), min_ptr, tuple_width) < 0) {
        new_min.assign(rhs_tuple.data(), tuple_width);
      } else {
        new_min.assign(min_ptr, tuple_width);
      }
      if (_tuple_cmp(rhs_tuple.data(), max_ptr, tuple_width) > 0) {
        new_max.assign(rhs_tuple.data(), tuple_width);
      } else {
        new_max.assign(max_ptr, tuple_width);
      }
      new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + out_hdr->count * stride);
      return true;
    }

    was_new_entry = true;
    auto* out_hdr = reinterpret_cast<TuplePayloadHeader*>(new_payload);
    out_hdr->count = 1;
    out_hdr->local_violation = 0;
    out_hdr->neighbor_flag = 0;
    out_hdr->tuple_width = tuple_width;
    auto* out_base = new_payload + sizeof(TuplePayloadHeader);
    std::memcpy(out_base, rhs_tuple.data(), tuple_width);
    const uint32_t rc_one = 1;
    std::memcpy(out_base + tuple_width, &rc_one, sizeof(rc_one));
    new_local = 0;
    new_min.assign(rhs_tuple.data(), tuple_width);
    new_max.assign(rhs_tuple.data(), tuple_width);
    new_len = static_cast<u16>(sizeof(TuplePayloadHeader) + _entry_stride(tuple_width));
    return true;
  }

 private:
  DependencyType _dependency_type;
  olc_detail::BTree _tree;
  olc_detail::VersionedGHistory _g_history;
  std::atomic<size_t> _total_entries{0};
};

BTreeOLCIndex::BTreeOLCIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index)
    : AbstractChunkIndex(ChunkIndexType::BTreeOLC), _indexed_segments(segments_to_index) {
  // Collect (value, offset) pairs from all segments.
  // For multi-segment composite keys we zip the segment values together.
  // For the common single-segment case this is straightforward.
  Assert(!segments_to_index.empty(), "BTreeOLCIndex requires at least one segment");

  const auto segment_count = segments_to_index.size();
  const auto row_count = segments_to_index[0]->size();

  // Build parallel (values-vector, offset) pairs
  using ValueRow = std::pair<std::vector<AllTypeVariant>, ChunkOffset>;
  std::vector<ValueRow> rows;
  rows.reserve(row_count);

  for (ChunkOffset i{0}; i < row_count; ++i) {
    std::vector<AllTypeVariant> vals;
    vals.reserve(segment_count);
    bool has_null = false;
    for (const auto& seg : segments_to_index) {
      auto v = (*seg)[i];
      if (variant_is_null(v)) {
        has_null = true;
        break;
      }
      vals.push_back(std::move(v));
    }
    if (has_null) {
      _null_positions.push_back(i);
    } else {
      rows.emplace_back(std::move(vals), i);
    }
  }

  // Sort rows by their values (lexicographic on the values vector).
  // std::variant operator< compares by type index first, then by value.
  // Since all entries in one column are the same type this is correct.
  std::sort(rows.begin(), rows.end(), [](const ValueRow& a, const ValueRow& b) {
    for (size_t col = 0; col < std::min(a.first.size(), b.first.size()); ++col) {
      if (a.first[col] < b.first[col])
        return true;
      if (b.first[col] < a.first[col])
        return false;
    }
    return a.first.size() < b.first.size();
  });

  _all_offsets.reserve(rows.size());
  _all_sorted_values.reserve(rows.size());
  for (auto& [vals, off] : rows) {
    // For single-column, store the single variant; for multi-column store first column.
    // The full composite key comparison is done in _lower_bound/_upper_bound.
    _all_sorted_values.push_back(std::move(vals[0]));
    _all_offsets.push_back(off);
  }
}

BTreeOLCIndex::~BTreeOLCIndex() = default;

// Helper: compare two AllTypeVariant using variant's natural ordering.
// For same-type variants this gives the correct column ordering.
static bool av_less(const AllTypeVariant& a, const AllTypeVariant& b) {
  return a < b;
}

AbstractChunkIndex::Iterator BTreeOLCIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  if (values.empty() || _all_sorted_values.empty())
    return _all_offsets.begin();
  const auto& target = values[0];
  auto it = std::lower_bound(_all_sorted_values.begin(), _all_sorted_values.end(), target,
                             [](const AllTypeVariant& elem, const AllTypeVariant& val) {
                               return av_less(elem, val);
                             });
  return _all_offsets.begin() + std::distance(_all_sorted_values.begin(), it);
}

AbstractChunkIndex::Iterator BTreeOLCIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  if (values.empty() || _all_sorted_values.empty())
    return _all_offsets.end();
  const auto& target = values[0];
  auto it = std::upper_bound(_all_sorted_values.begin(), _all_sorted_values.end(), target,
                             [](const AllTypeVariant& val, const AllTypeVariant& elem) {
                               return av_less(val, elem);
                             });
  return _all_offsets.begin() + std::distance(_all_sorted_values.begin(), it);
}

AbstractChunkIndex::Iterator BTreeOLCIndex::_cbegin() const {
  return _all_offsets.begin();
}

AbstractChunkIndex::Iterator BTreeOLCIndex::_cend() const {
  return _all_offsets.end();
}

std::vector<std::shared_ptr<const AbstractSegment>> BTreeOLCIndex::_get_indexed_segments() const {
  return _indexed_segments;
}

size_t BTreeOLCIndex::_memory_consumption() const {
  size_t bytes = 0;
  bytes += sizeof(*this);
  bytes += _all_offsets.capacity() * sizeof(ChunkOffset);
  bytes += _all_sorted_values.capacity() * sizeof(AllTypeVariant);
  bytes += _indexed_segments.capacity() * sizeof(std::shared_ptr<const AbstractSegment>);
  if (_val_tree) {
    // Rough estimate: each entry ~(BVH_SIZE + avg_rhs * 8) per key
    bytes += _val_tree->size() * (olc_detail::BVH_SIZE + 2 * sizeof(uint64_t) + 64);
  }
  if (_multi_val_state) {
    bytes += _multi_val_state->approximate_memory_usage();
  }
  return bytes;
}

size_t BTreeOLCIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset /*distinct_count*/,
                                                  uint32_t value_bytes) {
  // Rough estimate: sorted array overhead
  return row_count * (sizeof(ChunkOffset) + value_bytes);
}

// Validation API

uint64_t BTreeOLCIndex::_variant_to_u64(const AllTypeVariant& v) {
  struct Converter : public boost::static_visitor<uint64_t> {
    [[maybe_unused]] Converter() = default;

    uint64_t operator()(NullValue /*unused*/) const {
      return 0;
    }

    uint64_t operator()(int32_t val) const {
      return static_cast<uint64_t>(static_cast<uint32_t>(val) ^ 0x80000000u);
    }

    uint64_t operator()(int64_t val) const {
      return static_cast<uint64_t>(val) ^ 0x8000000000000000ull;
    }

    uint64_t operator()(float val) const {
      uint32_t bits;
      std::memcpy(&bits, &val, sizeof(bits));
      if (bits >> 31)
        bits = ~bits;
      else
        bits ^= 0x80000000u;
      return static_cast<uint64_t>(bits);
    }

    uint64_t operator()(double val) const {
      uint64_t bits;
      std::memcpy(&bits, &val, sizeof(bits));
      if (bits >> 63)
        bits = ~bits;
      else
        bits ^= 0x8000000000000000ull;
      return bits;
    }

    uint64_t operator()(const pmr_string& val) const {
      // FNV-1a hash -- order not preserved, but OD on strings is not common
      uint64_t h = 14695981039346656037ull;
      for (unsigned char c : val) {
        h ^= c;
        h *= 1099511628211ull;
      }
      return h;
    }
  };

  return boost::apply_visitor(Converter{}, v);
}

std::vector<uint64_t> BTreeOLCIndex::_variants_to_u64_tuple(const std::vector<AllTypeVariant>& values) {
  std::vector<uint64_t> tuple;
  tuple.reserve(values.size());
  for (const auto& value : values) {
    tuple.emplace_back(_variant_to_u64(value));
  }
  return tuple;
}

void BTreeOLCIndex::_ensure_val_tree(DependencyType dep_type) {
  std::call_once(_validation_dep_type_once, [this, dep_type]() {
    this->_val_tree_dep_type = dep_type;
    this->_val_tree_dep_type_initialized = true;
  });

  if (_val_tree_dep_type_initialized && _val_tree_dep_type != dep_type) {
    throw std::logic_error("BTreeOLCIndex validation tree cannot switch dependency type (FD/OD) after initialization");
  }
}

void BTreeOLCIndex::insert_entry_for_validation(const std::vector<AllTypeVariant>& lhs_values,
                                                const std::vector<AllTypeVariant>& rhs_values,
                                                DependencyType dep_type, CommitID commit_cid,
                                                std::optional<CommitID> lowest_active) {
  _ensure_val_tree(dep_type);

  if (lhs_values.size() == 1 && rhs_values.size() == 1) {
    std::call_once(_val_tree_init_once, [&]() {
      _val_tree =
          std::make_unique<olc_detail::DependencyValidatingBTree>(static_cast<olc_detail::DependencyType>(dep_type));
    });
    const uint64_t lhs_key = _variant_to_u64(lhs_values[0]);
    const uint64_t rhs_key = _variant_to_u64(rhs_values[0]);
    _val_tree->insert(lhs_key, rhs_key, commit_cid, lowest_active);
    return;
  }

  if (lhs_values.empty() || rhs_values.empty()) {
    throw std::logic_error("BTreeOLCIndex validation requires non-empty lhs and rhs value vectors");
  }

  std::call_once(_multi_val_state_init_once, [&]() {
    _multi_val_state = std::make_unique<MultiValidationState>(dep_type);
  });
  _multi_val_state->insert(_variants_to_u64_tuple(lhs_values), _variants_to_u64_tuple(rhs_values), commit_cid,
                            lowest_active);
}

void BTreeOLCIndex::delete_entry_for_validation(const std::vector<AllTypeVariant>& lhs_values,
                                                const std::vector<AllTypeVariant>& rhs_values,
                                                DependencyType dep_type, CommitID commit_cid,
                                                std::optional<CommitID> lowest_active) {
  _ensure_val_tree(dep_type);

  if (lhs_values.size() == 1 && rhs_values.size() == 1) {
    std::call_once(_val_tree_init_once, [&]() {
      _val_tree =
          std::make_unique<olc_detail::DependencyValidatingBTree>(static_cast<olc_detail::DependencyType>(dep_type));
    });
    const uint64_t lhs_key = _variant_to_u64(lhs_values[0]);
    const uint64_t rhs_key = _variant_to_u64(rhs_values[0]);
    _val_tree->remove(lhs_key, rhs_key, nullptr, commit_cid, lowest_active);
    return;
  }

  if (lhs_values.empty() || rhs_values.empty()) {
    throw std::logic_error("BTreeOLCIndex validation requires non-empty lhs and rhs value vectors");
  }

  std::call_once(_multi_val_state_init_once, [&]() {
    _multi_val_state = std::make_unique<MultiValidationState>(dep_type);
  });
  _multi_val_state->remove(_variants_to_u64_tuple(lhs_values), _variants_to_u64_tuple(rhs_values), commit_cid,
                            lowest_active);
}

int BTreeOLCIndex::global_violation_count() const {
  int violations = 0;
  if (_val_tree) {
    violations += _val_tree->global_violation_count();
  }
  if (_multi_val_state) {
    violations += _multi_val_state->global_violation_count();
  }
  return violations;
}

int BTreeOLCIndex::global_violation_count(CommitID snapshot_cid) const {
  int violations = 0;
  if (_val_tree) {
    violations += _val_tree->global_violation_count(snapshot_cid);
  }
  if (_multi_val_state) {
    violations += _multi_val_state->global_violation_count(snapshot_cid);
  }
  return violations;
}

bool BTreeOLCIndex::dependency_holds() const {
  return global_violation_count() == 0;
}

bool BTreeOLCIndex::dependency_holds(CommitID snapshot_cid) const {
  return global_violation_count(snapshot_cid) == 0;
}

}  // namespace hyrise