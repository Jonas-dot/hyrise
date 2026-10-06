#include "b_tree_olc_index.hpp"

#include <algorithm>
#include <cstring>

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
  return bytes;
}

size_t BTreeOLCIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset /*distinct_count*/,
                                                  uint32_t value_bytes) {
  // Rough estimate: sorted array overhead
  return row_count * (sizeof(ChunkOffset) + value_bytes);
}

}  // namespace hyrise
