/**
 * BTree OLC Implementation
 * Ported from btree24/btree/vmcache_btree.cpp (SIGMOD 2025)
 * 
 * Modified to use Eager Splitting instead of ensure_space():
 * Inner nodes are split during traversal if they are too full,
 * avoiding the complex ensure_space() logic that can cause livelocks.
 */

#include "btree_olc.hpp"
#include <memory>

namespace btree_olc {

// Thread-local buffer for key operations to avoid VLA stack overflow
// PAGE_SIZE is max key size since keys must fit in a page
static thread_local u8 tls_key_buffer[PAGE_SIZE];

// Node Operations

void BTreeNode::compactify() {
  BTreeNode tmp(is_leaf);
  tmp.set_fences(get_lower_fence(), lower_fence.len, get_upper_fence(), upper_fence.len);
  copy_key_value_range(&tmp, 0, 0, count);
  tmp.upper_child = upper_child;
  u64 ofs = sizeof(OptimisticLock);
  std::memcpy(ptr() + ofs, tmp.ptr() + ofs, sizeof(BTreeNode) - ofs);
  make_hints();
}

void BTreeNode::copy_key_value_range(BTreeNode* dst, u16 dst_slot, u16 src_slot, u16 src_count) {
  if (prefix_len <= dst->prefix_len) {
    u16 diff = dst->prefix_len - prefix_len;
    for (u16 i = 0; i < src_count; ++i) {
      u16 new_key_len = slots[src_slot + i].key_len - diff;
      u16 space = new_key_len + slots[src_slot + i].payload_len;
      dst->data_offset -= space;
      dst->space_used += space;
      dst->slots[dst_slot + i].offset = dst->data_offset;
      const u8* key = get_key(src_slot + i) + diff;
      std::memcpy(dst->get_key(dst_slot + i), key, space);
      dst->slots[dst_slot + i].head = compute_head(key, new_key_len);
      dst->slots[dst_slot + i].key_len = new_key_len;
      dst->slots[dst_slot + i].payload_len = slots[src_slot + i].payload_len;
    }
  } else {
    // Use thread-local buffer instead of VLA to avoid stack overflow
    for (u16 i = 0; i < src_count; ++i) {
      u16 full_len = slots[src_slot + i].key_len + prefix_len;
      assert(full_len <= PAGE_SIZE);
      std::memcpy(tls_key_buffer, get_lower_fence(), prefix_len);
      std::memcpy(tls_key_buffer + prefix_len, get_key(src_slot + i), slots[src_slot + i].key_len);
      dst->store_key_value(dst_slot + i, tls_key_buffer, full_len, 
                           get_payload(src_slot + i), slots[src_slot + i].payload_len);
    }
  }
  dst->count += src_count;
}

void BTreeNode::split_node(BTreeNode* parent, u16 sep_slot, const u8* sep, u16 sep_len) {
  assert(sep_slot > 0);
  BTreeNode tmp(is_leaf);
  BTreeNode* left = &tmp;
  BTreeNode* right = BTreeNode::alloc(is_leaf);

  left->set_fences(get_lower_fence(), lower_fence.len, sep, sep_len);
  right->set_fences(sep, sep_len, get_upper_fence(), upper_fence.len);

  // Update parent: old slot points to right, insert separator pointing to left
  u16 old_parent_slot = parent->lower_bound(sep, sep_len);
  if (old_parent_slot == parent->count) {
    assert(parent->upper_child == this);  // btree24: assert(parent->upperInnerNode == leftPID)
    parent->upper_child = right;
  } else {
    assert(parent->get_child(old_parent_slot) == this);  // btree24: assert(parent->getChild(oldParentSlot) == leftPID)
    std::memcpy(parent->get_payload(old_parent_slot), &right, sizeof(right));
  }
  BTreeNode* self = this;
  parent->insert_in_page(sep, sep_len, reinterpret_cast<u8*>(&self), sizeof(BTreeNode*));

  if (is_leaf) {
    copy_key_value_range(left, 0, 0, sep_slot + 1);
    copy_key_value_range(right, 0, left->count, count - left->count);
    left->next_leaf  = right;
    right->next_leaf = this->next_leaf;
    left->prev_leaf  = this->prev_leaf;  // copied to `this` by the memcpy below
    // right->prev_leaf is set to `this` AFTER the memcpy -- `left` is a stack temp
  } else {
    // Inner node: separator moves to parent
    copy_key_value_range(left, 0, 0, sep_slot);
    copy_key_value_range(right, 0, left->count + 1, count - left->count - 1);
    left->upper_child = get_child(left->count);
    right->upper_child = upper_child;
  }

  left->make_hints();
  right->make_hints();
  u64 ofs = sizeof(OptimisticLock);
  std::memcpy(ptr() + ofs, left->ptr() + ofs, sizeof(BTreeNode) - ofs);
  // After memcpy: `this` is the left half. Set right->prev_leaf to `this` (the
  // actual heap address), NOT to `left` which was a stack-allocated temporary.
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

// BTree Operations -- matching btree24's split/merge approach

void BTree::try_split(WriteGuard&& node_guard, WriteGuard&& parent_guard,
                      const u8* key, u16 key_len, u16 payload_len) {
  BTreeNode* node = node_guard.node();
  BTreeNode* parent = parent_guard.node();

  if (!parent) {
    BTreeNode* new_root = BTreeNode::alloc(false);
    new_root->upper_child = node;
    parent_guard = WriteGuard(new_root);  // lock BEFORE publishing
    _root.store(new_root, std::memory_order_release);
    parent = new_root;
  }

  if (node->count <= 1)
    return;

  auto sep_info = node->find_separator();
  assert(sep_info.length <= sizeof(BTreeNode));
  u8 sep_key[sep_info.length];
  node->get_separator(sep_key, sep_info);

  if (parent->has_space_for(sep_info.length, sizeof(BTreeNode*))) {
    node->split_node(parent, sep_info.slot, sep_key, sep_info.length);

    // Update old_next->prev_leaf to point to the new right half.
    // After split_node: node (= left half) -> next_leaf -> right -> next_leaf -> old_next.
    // We hold write locks on parent + node. Acquiring old_next's write lock is
    // deadlock-free: any thread holding old_next and needing parent will throw
    // OLCRestartException (parent's version changed) and restart.
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
  ensure_space(parent_ptr, sep_key, sep_info.length, sizeof(BTreeNode*));
}

void BTree::ensure_space(BTreeNode* to_split, const u8* key, u16 key_len, u16 payload_len) {
  for (u64 retry = 0;; ++retry) {
    try {
      BTreeNode* parent = nullptr;
      u64 parent_v = 0;
      BTreeNode* node = _root.load(std::memory_order_acquire);

      while (!node->is_leaf && node != to_split) {
        u64 v = node->lock.read_lock_or_restart();
        parent = node;
        parent_v = v;
        BTreeNode* child = node->lookup_inner(key, key_len);
        node->lock.check_or_restart(v);
        node = child;
      }

      if (node == to_split) {
        u64 v = node->lock.read_lock_or_restart();
        if (node->has_space_for(key_len, payload_len)) {
          node->lock.check_or_restart(v);
          return;
        }
        WriteGuard parent_guard;
        if (parent)
          parent_guard = WriteGuard(parent, parent_v);
        WriteGuard node_guard(node, v);
        try_split(std::move(node_guard), std::move(parent_guard), key, key_len, payload_len);
      }
      return;
    } catch (const OLCRestartException&) {
      olc_yield(retry);
    }
  }
}

}  // namespace btree_olc