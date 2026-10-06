#include "btree.hpp"

// Adapted TUM btree-cpp page engine. Dependency semantics, MVCC history, and
// commit coordination live above this layer and are not part of the takeover.

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstring>
#include <memory>
#include <new>
#include <random>
#include <stdexcept>
#include <thread>

namespace hyrise::dv_tree::engine {
namespace {

#ifdef DV_TESTING
std::atomic<bool> fail_next_page_publication{false};
#endif

uint32_t key_head(const uint8_t* key, unsigned length) {
  uint32_t result = 0;
  for (unsigned i = 0; i < std::min(length, 4u); ++i) {
    result |= static_cast<uint32_t>(key[i]) << (24u - 8u * i);
  }
  return result;
}

void olc_backoff(uint64_t retry) {
  if (retry < 6) {
    std::this_thread::yield();
    return;
  }
  thread_local std::minstd_rand generator(std::random_device{}());
  const uint64_t cap = std::min<uint64_t>(retry * 2, 50);
  std::uniform_int_distribution<uint64_t> delay(0, cap);
  std::this_thread::sleep_for(std::chrono::microseconds(delay(generator)));
}

void load_view(const Node* source, Node& view) {
  const std::shared_ptr<const PageBody> image = source->published_page();
  if (!image)
    throw RestartOperation{};
  std::memcpy(view.page.bytes, image->bytes, PAGE_SIZE);
}

}  // namespace

Node::Node(bool leaf, bool publish_initial, NodeAccounting* accounting)
    : accounting_(accounting), accounted_leaf_(leaf) {
  ::new (static_cast<void*>(page.bytes)) PageHeader(leaf);
  if (publish_initial)
    publish_page();
  if (accounting_ != nullptr) {
    accounting_->nodes.fetch_add(1, std::memory_order_relaxed);
    (leaf ? accounting_->leaf_nodes : accounting_->inner_nodes).fetch_add(1, std::memory_order_relaxed);
  }
}

Node::~Node() {
  if (accounting_ == nullptr)
    return;
  accounting_->nodes.fetch_sub(1, std::memory_order_relaxed);
  (accounted_leaf_ ? accounting_->leaf_nodes : accounting_->inner_nodes).fetch_sub(1, std::memory_order_relaxed);
}

uint64_t Node::read_version_or_restart() const {
  const uint64_t version = control.version_lock.load(std::memory_order_acquire);
  if ((version & ~NodeControl::VERSION_MASK) == NodeControl::LOCKED_STATE) {
    throw RestartOperation{};
  }
  return version;
}

void Node::check_version_or_restart(uint64_t version) const {
  std::atomic_thread_fence(std::memory_order_acquire);
  if (control.version_lock.load(std::memory_order_relaxed) != version) {
    throw RestartOperation{};
  }
}

bool Node::try_write_lock(uint64_t expected) {
  if ((expected & ~NodeControl::VERSION_MASK) != 0)
    return false;
  const uint64_t locked = (expected & NodeControl::VERSION_MASK) | NodeControl::LOCKED_STATE;
  return control.version_lock.compare_exchange_strong(expected, locked, std::memory_order_acquire,
                                                      std::memory_order_relaxed);
}

void Node::write_lock() {
  for (uint64_t retry = 0;; ++retry) {
    uint64_t expected = control.version_lock.load(std::memory_order_relaxed);
    if ((expected & ~NodeControl::VERSION_MASK) == 0 && try_write_lock(expected))
      return;
    olc_backoff(retry);
  }
}

void Node::write_unlock() {
  const uint64_t locked = control.version_lock.load(std::memory_order_relaxed);
  assert((locked & ~NodeControl::VERSION_MASK) == NodeControl::LOCKED_STATE);
  const uint64_t next_version = ((locked & NodeControl::VERSION_MASK) + 1) & NodeControl::VERSION_MASK;
  control.version_lock.store(next_version, std::memory_order_release);
}

std::shared_ptr<const PageBody> Node::published_page() const {
  return std::atomic_load_explicit(&control.published_page, std::memory_order_acquire);
}

std::shared_ptr<PageBody> Node::allocate_page_image() {
#ifdef DV_TESTING
  if (fail_next_page_publication.exchange(false, std::memory_order_acq_rel)) {
    throw std::bad_alloc{};
  }
#endif
  return std::make_shared<PageBody>();
}

void Node::publish_page() {
  publish_page(allocate_page_image());
}

void Node::publish_page(std::shared_ptr<PageBody> image) noexcept {
  std::memcpy(image->bytes, page.bytes, PAGE_SIZE);
  std::shared_ptr<const PageBody> immutable = std::move(image);
  std::atomic_store_explicit(&control.published_page, std::move(immutable), std::memory_order_release);
}

#ifdef DV_TESTING
void Node::fail_next_page_publication_for_test() {
  fail_next_page_publication.store(true, std::memory_order_release);
}
#endif

NodeWriteGuard::NodeWriteGuard(Node* node) : publication_buffer_(Node::allocate_page_image()), node_(node) {
  node_->write_lock();
}

NodeWriteGuard::NodeWriteGuard(Node* node, uint64_t expected)
    : publication_buffer_(Node::allocate_page_image()), node_(node) {
  if (!node_->try_write_lock(expected)) {
    node_ = nullptr;
    throw RestartOperation{};
  }
}

NodeWriteGuard::~NodeWriteGuard() {
  release();
}

NodeWriteGuard::NodeWriteGuard(NodeWriteGuard&& other) noexcept
    : publication_buffer_(std::move(other.publication_buffer_)), node_(other.node_) {
  other.node_ = nullptr;
}

NodeWriteGuard& NodeWriteGuard::operator=(NodeWriteGuard&& other) noexcept {
  if (this == &other)
    return *this;
  release();
  publication_buffer_ = std::move(other.publication_buffer_);
  node_ = other.node_;
  other.node_ = nullptr;
  return *this;
}

void NodeWriteGuard::release() noexcept {
  if (node_ == nullptr)
    return;
  node_->publish_page(std::move(publication_buffer_));
  node_->write_unlock();
  node_ = nullptr;
}

PageHeader& Node::header() {
  return *reinterpret_cast<PageHeader*>(page.bytes);
}

const PageHeader& Node::header() const {
  return *reinterpret_cast<const PageHeader*>(page.bytes);
}

Slot* Node::slots() {
  return reinterpret_cast<Slot*>(page.bytes + sizeof(PageHeader));
}

const Slot* Node::slots() const {
  return reinterpret_cast<const Slot*>(page.bytes + sizeof(PageHeader));
}

uint8_t* Node::key(unsigned slot) {
  return page.bytes + slots()[slot].offset;
}

const uint8_t* Node::key(unsigned slot) const {
  return page.bytes + slots()[slot].offset;
}

uint8_t* Node::payload_bytes(unsigned slot) {
  return key(slot) + slots()[slot].key_length;
}

const uint8_t* Node::payload_bytes(unsigned slot) const {
  return key(slot) + slots()[slot].key_length;
}

void* Node::payload_value(unsigned slot) const {
  void* value = nullptr;
  std::memcpy(&value, payload_bytes(slot), PAYLOAD_SIZE);
  return value;
}

Node* Node::child(unsigned slot) const {
  return static_cast<Node*>(payload_value(slot));
}

uint8_t* Node::lower_fence_bytes() {
  return page.bytes + header().lower_fence.offset;
}

uint8_t* Node::upper_fence_bytes() {
  return page.bytes + header().upper_fence.offset;
}

const uint8_t* Node::prefix() const {
  return page.bytes + header().lower_fence.offset;
}

unsigned Node::free_space() const {
  const auto* directory_end = reinterpret_cast<const uint8_t*>(slots() + header().count);
  return static_cast<unsigned>((page.bytes + header().data_offset) - directory_end);
}

unsigned Node::free_space_after_compaction() const {
  const auto* directory_end = reinterpret_cast<const uint8_t*>(slots() + header().count);
  return PAGE_SIZE - static_cast<unsigned>(directory_end - page.bytes) - header().space_used;
}

unsigned Node::space_needed(unsigned key_length) const {
  assert(key_length >= header().prefix_length);
  return sizeof(Slot) + key_length - header().prefix_length + PAYLOAD_SIZE;
}

bool Node::request_space(unsigned bytes_needed) {
  if (bytes_needed <= free_space())
    return true;
  if (bytes_needed <= free_space_after_compaction()) {
    compactify();
    return true;
  }
  return false;
}

void Node::make_hints() {
  if (header().count == 0) {
    std::fill(std::begin(header().hints), std::end(header().hints), 0);
    return;
  }
  const unsigned distance = header().count / (HINT_COUNT + 1);
  for (unsigned i = 0; i < HINT_COUNT; ++i) {
    header().hints[i] = slots()[distance * (i + 1)].head;
  }
}

void Node::update_hints(unsigned inserted_slot) {
  const unsigned distance = header().count / (HINT_COUNT + 1);
  unsigned begin = 0;
  if (header().count > HINT_COUNT * 2 + 1 && ((header().count - 1) / (HINT_COUNT + 1)) == distance &&
      inserted_slot / distance > 1) {
    begin = inserted_slot / distance - 1;
  }
  for (unsigned i = begin; i < HINT_COUNT; ++i) {
    header().hints[i] = slots()[distance * (i + 1)].head;
  }
}

void Node::search_hints(uint32_t search_head, unsigned& lower, unsigned& upper_bound) const {
  if (header().count <= HINT_COUNT * 2)
    return;
  const unsigned distance = upper_bound / (HINT_COUNT + 1);
  unsigned first = 0;
  while (first < HINT_COUNT && header().hints[first] < search_head)
    ++first;
  unsigned last = first;
  while (last < HINT_COUNT && header().hints[last] == search_head)
    ++last;
  lower = first * distance;
  if (last < HINT_COUNT)
    upper_bound = (last + 1) * distance;
}

unsigned Node::lower_bound(const uint8_t* search_key, unsigned key_length, bool& found) const {
  found = false;
  const unsigned prefix_length = header().prefix_length;
  // An optimistic traversal may have selected a child from the prior parent
  // image immediately before that child becomes the right half of a split.
  // Its immutable page image is memory-safe to inspect, but its new fence
  // prefix need not contain this search key. Treat that logical mismatch as
  // an OLC validation failure; asserting here aborts before the caller can
  // validate the node version and restart.
  if (key_length < prefix_length || std::memcmp(search_key, prefix(), prefix_length) != 0) {
    throw RestartOperation{};
  }
  search_key += header().prefix_length;
  key_length -= header().prefix_length;

  unsigned lower = 0;
  unsigned upper_bound = header().count;
  const uint32_t search_head = key_head(search_key, key_length);
  search_hints(search_head, lower, upper_bound);

  while (lower < upper_bound) {
    const unsigned middle = lower + (upper_bound - lower) / 2;
    const Slot& stored = slots()[middle];
    if (search_head < stored.head) {
      upper_bound = middle;
    } else if (search_head > stored.head) {
      lower = middle + 1;
    } else {
      const int comparison =
          std::memcmp(search_key, key(middle), std::min(key_length, static_cast<unsigned>(stored.key_length)));
      if (comparison < 0 || (comparison == 0 && key_length < stored.key_length)) {
        upper_bound = middle;
      } else if (comparison > 0 || (comparison == 0 && key_length > stored.key_length)) {
        lower = middle + 1;
      } else {
        found = true;
        return middle;
      }
    }
  }
  return lower;
}

unsigned Node::lower_bound(const uint8_t* search_key, unsigned key_length) const {
  bool ignored = false;
  return lower_bound(search_key, key_length, ignored);
}

Node* Node::lookup_inner(const uint8_t* search_key, unsigned key_length) const {
  const unsigned position = lower_bound(search_key, key_length);
  return position == header().count ? header().upper : child(position);
}

bool Node::insert(const uint8_t* full_key, unsigned key_length, void* value) {
  if (!request_space(space_needed(key_length)))
    return false;
  bool found = false;
  const unsigned position = lower_bound(full_key, key_length, found);
  if (found)
    throw DuplicateKeyError("duplicate key passed to basic B-tree engine");
  std::memmove(slots() + position + 1, slots() + position, sizeof(Slot) * (header().count - position));
  store(position, full_key, key_length, value);
  ++header().count;
  update_hints(position);
  return true;
}

bool Node::insert_child(const uint8_t* separator, unsigned separator_length, Node* new_child) {
  return insert(separator, separator_length, new_child);
}

void Node::store(unsigned position, const uint8_t* full_key, unsigned key_length, void* value) {
  full_key += header().prefix_length;
  key_length -= header().prefix_length;
  Slot& target = slots()[position];
  target.head = key_head(full_key, key_length);
  target.key_length = static_cast<uint16_t>(key_length);
  const unsigned bytes_needed = key_length + PAYLOAD_SIZE;
  header().data_offset = static_cast<uint16_t>(header().data_offset - bytes_needed);
  header().space_used = static_cast<uint16_t>(header().space_used + bytes_needed);
  target.offset = header().data_offset;
  assert(key(position) >= reinterpret_cast<uint8_t*>(slots() + position + 1));
  std::memcpy(key(position), full_key, key_length);
  std::memcpy(payload_bytes(position), &value, PAYLOAD_SIZE);
}

void Node::insert_fence(FenceKeySlot& destination, const uint8_t* fence, unsigned length) {
  assert(free_space() >= length);
  header().data_offset = static_cast<uint16_t>(header().data_offset - length);
  header().space_used = static_cast<uint16_t>(header().space_used + length);
  destination.offset = header().data_offset;
  destination.length = static_cast<uint16_t>(length);
  std::memcpy(page.bytes + header().data_offset, fence, length);
}

void Node::set_fences(const uint8_t* lower, unsigned lower_length, const uint8_t* upper_bound, unsigned upper_length) {
  insert_fence(header().lower_fence, lower, lower_length);
  insert_fence(header().upper_fence, upper_bound, upper_length);
  header().prefix_length = 0;
  while (header().prefix_length < std::min(lower_length, upper_length) &&
         lower[header().prefix_length] == upper_bound[header().prefix_length]) {
    ++header().prefix_length;
  }
}

void Node::copy_entry(unsigned source_slot, Node* destination, unsigned destination_slot) const {
  const unsigned full_length = slots()[source_slot].key_length + header().prefix_length;
  std::array<uint8_t, MAX_KEY_LENGTH> restored{};
  std::memcpy(restored.data(), prefix(), header().prefix_length);
  std::memcpy(restored.data() + header().prefix_length, key(source_slot), slots()[source_slot].key_length);
  destination->store(destination_slot, restored.data(), full_length, payload_value(source_slot));
}

void Node::copy_range(Node* destination, unsigned destination_slot, unsigned source_slot, unsigned source_count) const {
  if (header().prefix_length <= destination->header().prefix_length) {
    const unsigned difference = destination->header().prefix_length - header().prefix_length;
    for (unsigned i = 0; i < source_count; ++i) {
      const Slot& source = slots()[source_slot + i];
      assert(source.key_length >= difference);
      const unsigned new_key_length = source.key_length - difference;
      const unsigned bytes_needed = new_key_length + PAYLOAD_SIZE;
      destination->header().data_offset = static_cast<uint16_t>(destination->header().data_offset - bytes_needed);
      destination->header().space_used = static_cast<uint16_t>(destination->header().space_used + bytes_needed);
      Slot& target = destination->slots()[destination_slot + i];
      target.offset = destination->header().data_offset;
      target.key_length = static_cast<uint16_t>(new_key_length);
      const uint8_t* shortened_key = key(source_slot + i) + difference;
      target.head = key_head(shortened_key, new_key_length);
      std::memcpy(destination->key(destination_slot + i), shortened_key, bytes_needed);
    }
  } else {
    for (unsigned i = 0; i < source_count; ++i) {
      copy_entry(source_slot + i, destination, destination_slot + i);
    }
  }
  destination->header().count = static_cast<uint16_t>(destination->header().count + source_count);
  assert(destination->page.bytes + destination->header().data_offset >=
         reinterpret_cast<uint8_t*>(destination->slots() + destination->header().count));
}

void Node::compactify() {
  const unsigned expected = free_space_after_compaction();
  static_cast<void>(expected);
  Node compacted(is_leaf(), false);
  compacted.set_fences(lower_fence_bytes(), header().lower_fence.length, upper_fence_bytes(),
                       header().upper_fence.length);
  copy_range(&compacted, 0, 0, header().count);
  compacted.header().upper = header().upper;
  std::memcpy(page.bytes, compacted.page.bytes, PAGE_SIZE);
  make_hints();
  assert(free_space() == expected);
}

unsigned Node::common_prefix(unsigned left_slot, unsigned right_slot) const {
  assert(left_slot < header().count && right_slot < header().count);
  const unsigned limit = std::min<unsigned>(slots()[left_slot].key_length, slots()[right_slot].key_length);
  unsigned length = 0;
  while (length < limit && key(left_slot)[length] == key(right_slot)[length])
    ++length;
  return length;
}

Node::SeparatorInfo Node::find_separator() const {
  assert(header().count > 1);
  if (!is_leaf()) {
    const unsigned separator = header().count / 2 - 1;
    return {static_cast<unsigned>(header().prefix_length + slots()[separator].key_length), separator, false};
  }

  const unsigned lower = header().count / 2 - header().count / 32;
  const unsigned upper_bound = lower + header().count / 16;
  const unsigned range_prefix = common_prefix(lower, upper_bound);
  if (slots()[lower].key_length == range_prefix) {
    return {static_cast<unsigned>(header().prefix_length + range_prefix), lower, false};
  }
  for (unsigned i = lower + 1; i <= upper_bound; ++i) {
    if (key(i)[range_prefix] != key(lower)[range_prefix]) {
      if (slots()[i].key_length == range_prefix + 1) {
        return {static_cast<unsigned>(header().prefix_length + range_prefix + 1), i, false};
      }
      return {static_cast<unsigned>(header().prefix_length + range_prefix + 1), i - 1, true};
    }
  }
  throw std::logic_error("separator search failed inside its bounded window");
}

void Node::restore_key(uint8_t* destination, unsigned length, unsigned position) const {
  std::memcpy(destination, prefix(), header().prefix_length);
  std::memcpy(destination + header().prefix_length, key(position), length - header().prefix_length);
}

void Node::separator_key(uint8_t* destination, SeparatorInfo separator) const {
  restore_key(destination, separator.length, separator.slot + (separator.truncated ? 1u : 0u));
}

bool Node::split_with_parent(Node* parent) {
  const SeparatorInfo separator = find_separator();
  if (!parent->request_space(parent->space_needed(separator.length)))
    return false;
  std::array<uint8_t, MAX_KEY_LENGTH> separator_bytes{};
  separator_key(separator_bytes.data(), separator);
  split(parent, separator.slot, separator_bytes.data(), separator.length);
  return true;
}

void Node::split(Node* parent, unsigned separator_slot, const uint8_t* separator, unsigned separator_length,
                 bool previous_leaf_is_locked) {
  assert(separator_slot > 0);
  auto left_owner = std::make_unique<Node>(is_leaf(), false, accounting_);
  Node* left = left_owner.get();
  NodeWriteGuard left_guard(left);  // exclusive construction until the full relink is complete
  left->set_fences(lower_fence_bytes(), header().lower_fence.length, separator, separator_length);
  Node right(is_leaf(), false);
  right.set_fences(separator, separator_length, upper_fence_bytes(), header().upper_fence.length);

  // Acquire every potentially needed page guard before changing the parent or
  // sibling links. Guard construction allocates its publication image first;
  // an allocation failure therefore leaves the original tree untouched.
  Node* previous = nullptr;
  NodeWriteGuard previous_guard;
  if (is_leaf()) {
    previous = control.previous_leaf.load(std::memory_order_relaxed);
    if (previous != nullptr && !previous_leaf_is_locked)
      previous_guard = NodeWriteGuard(previous);
  }

  if (!parent->insert_child(separator, separator_length, left)) {
    throw std::logic_error("parent had no room after successful space reservation");
  }
  // The parent owns the new child from this point onward. All operations below
  // are allocation-free and non-throwing.
  static_cast<void>(left_owner.release());

  if (is_leaf()) {
    copy_range(left, 0, 0, separator_slot + 1);
    copy_range(&right, 0, left->header().count, header().count - left->header().count);

    // The existing node becomes the right half. The new left page is fully
    // published before any sibling/root path can make it observable.
    left->control.previous_leaf.store(previous, std::memory_order_relaxed);
    left->control.next_leaf.store(this, std::memory_order_relaxed);
    if (previous != nullptr) {
      previous->control.next_leaf.store(left, std::memory_order_relaxed);
    }
    control.previous_leaf.store(left, std::memory_order_relaxed);
  } else {
    copy_range(left, 0, 0, separator_slot);
    copy_range(&right, 0, left->header().count + 1, header().count - left->header().count - 1);
    left->header().upper = child(left->header().count);
    right.header().upper = header().upper;
  }
  left->make_hints();
  right.make_hints();
  std::memcpy(page.bytes, right.page.bytes, PAGE_SIZE);
}

bool Node::visit(const std::function<bool(const uint8_t*, unsigned, void*)>& callback) const {
  if (!is_leaf()) {
    for (unsigned i = 0; i < header().count; ++i) {
      if (!child(i)->visit(callback))
        return false;
    }
    return header().upper->visit(callback);
  }

  std::array<uint8_t, MAX_KEY_LENGTH> restored{};
  for (unsigned i = 0; i < header().count; ++i) {
    const unsigned full_length = header().prefix_length + slots()[i].key_length;
    std::memcpy(restored.data(), prefix(), header().prefix_length);
    std::memcpy(restored.data() + header().prefix_length, key(i), slots()[i].key_length);
    if (!callback(restored.data(), full_length, payload_value(i)))
      return false;
  }
  return true;
}

BTree::BTree() : root_(new Node(true, true, &accounting_)) {}

BTree::~BTree() {
  destroy(root_.load(std::memory_order_relaxed));
}

#ifdef DV_TESTING
void BTree::force_restart_once(RestartPoint point) {
  switch (point) {
    case RestartPoint::LookupLeaf:
      force_lookup_restart_.store(true, std::memory_order_release);
      return;
    case RestartPoint::NeighborSiblings:
      force_neighbor_restart_.store(true, std::memory_order_release);
      return;
    case RestartPoint::InsertLeaf:
      force_insert_restart_.store(true, std::memory_order_release);
      return;
  }
}

uint64_t BTree::restart_count(RestartPoint point) const {
  switch (point) {
    case RestartPoint::LookupLeaf:
      return lookup_restarts_.load(std::memory_order_relaxed);
    case RestartPoint::NeighborSiblings:
      return neighbor_restarts_.load(std::memory_order_relaxed);
    case RestartPoint::InsertLeaf:
      return insert_restarts_.load(std::memory_order_relaxed);
  }
  return 0;
}

bool BTree::consume_forced_restart(RestartPoint point) const {
  switch (point) {
    case RestartPoint::LookupLeaf:
      return force_lookup_restart_.exchange(false, std::memory_order_acq_rel);
    case RestartPoint::NeighborSiblings:
      return force_neighbor_restart_.exchange(false, std::memory_order_acq_rel);
    case RestartPoint::InsertLeaf:
      return force_insert_restart_.exchange(false, std::memory_order_acq_rel);
  }
  return false;
}

void BTree::count_restart(RestartPoint point) const {
  switch (point) {
    case RestartPoint::LookupLeaf:
      lookup_restarts_.fetch_add(1, std::memory_order_relaxed);
      return;
    case RestartPoint::NeighborSiblings:
      neighbor_restarts_.fetch_add(1, std::memory_order_relaxed);
      return;
    case RestartPoint::InsertLeaf:
      insert_restarts_.fetch_add(1, std::memory_order_relaxed);
      return;
  }
}
#endif

void BTree::destroy(Node* node) {
  if (!node->is_leaf()) {
    for (unsigned i = 0; i < node->count(); ++i)
      destroy(node->child(i));
    destroy(node->upper());
  }
  delete node;
}

Node* BTree::find_leaf(const uint8_t* key, unsigned key_length) const {
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* node = root_.load(std::memory_order_acquire);
      uint64_t version = node->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != node)
        throw RestartOperation{};
      for (;;) {
        Node view(true, false);
        load_view(node, view);
        if (view.is_leaf()) {
          node->check_version_or_restart(version);
          return node;
        }
        Node* child = view.lookup_inner(key, key_length);
        node->check_version_or_restart(version);  // validate before child dereference
        const uint64_t child_version = child->read_version_or_restart();
        node->check_version_or_restart(version);
        node = child;
        version = child_version;
      }
    } catch (const RestartOperation&) {
      olc_backoff(retry);
    }
  }
}

Node* BTree::first_leaf() const {
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* node = root_.load(std::memory_order_acquire);
      uint64_t version = node->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != node)
        throw RestartOperation{};
      for (;;) {
        Node view(true, false);
        load_view(node, view);
        if (view.is_leaf()) {
          node->check_version_or_restart(version);
          return node;
        }
        Node* child = view.child(0);
        node->check_version_or_restart(version);
        const uint64_t child_version = child->read_version_or_restart();
        node->check_version_or_restart(version);
        node = child;
        version = child_version;
      }
    } catch (const RestartOperation&) {
      olc_backoff(retry);
    }
  }
}

void* BTree::lookup(const uint8_t* search_key, unsigned key_length) const {
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* node = root_.load(std::memory_order_acquire);
      uint64_t version = node->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != node)
        throw RestartOperation{};
      for (;;) {
        Node view(true, false);
        load_view(node, view);
        if (!view.is_leaf()) {
          Node* child = view.lookup_inner(search_key, key_length);
          node->check_version_or_restart(version);
          const uint64_t child_version = child->read_version_or_restart();
          node->check_version_or_restart(version);
          node = child;
          version = child_version;
          continue;
        }

        bool found = false;
        const unsigned position = view.lower_bound(search_key, key_length, found);
        void* result = found ? view.payload_value(position) : nullptr;
#ifdef DV_TESTING
        if (consume_forced_restart(RestartPoint::LookupLeaf))
          throw RestartOperation{};
#endif
        node->check_version_or_restart(version);
        return result;
      }
    } catch (const RestartOperation&) {
#ifdef DV_TESTING
      count_restart(RestartPoint::LookupLeaf);
#endif
      olc_backoff(retry);
    }
  }
}

Node::Neighbors BTree::neighbors(const uint8_t* absent_key, unsigned key_length) const {
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* leaf = root_.load(std::memory_order_acquire);
      uint64_t version = leaf->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != leaf)
        throw RestartOperation{};
      Node view(true, false);
      for (;;) {
        load_view(leaf, view);
        if (view.is_leaf())
          break;
        Node* child = view.lookup_inner(absent_key, key_length);
        leaf->check_version_or_restart(version);
        const uint64_t child_version = child->read_version_or_restart();
        leaf->check_version_or_restart(version);
        leaf = child;
        version = child_version;
      }
      bool found = false;
      const unsigned position = view.lower_bound(absent_key, key_length, found);
      if (found) {
        leaf->check_version_or_restart(version);
        throw std::logic_error("neighbors expects an absent key");
      }

      Node::Neighbors result;
      Node* previous = nullptr;
      Node* next = nullptr;
      if (position > 0)
        result.predecessor = view.payload_value(position - 1);
      else
        previous = leaf->control.previous_leaf.load(std::memory_order_relaxed);
      if (position < view.count())
        result.successor = view.payload_value(position);
      else
        next = leaf->control.next_leaf.load(std::memory_order_relaxed);

      uint64_t previous_version = 0;
      uint64_t next_version = 0;
      if (previous != nullptr)
        previous_version = previous->read_version_or_restart();
      if (next != nullptr)
        next_version = next->read_version_or_restart();
#ifdef DV_TESTING
      if (consume_forced_restart(RestartPoint::NeighborSiblings))
        throw RestartOperation{};
#endif
      leaf->check_version_or_restart(version);

      if (previous != nullptr) {
        Node previous_view(true, false);
        load_view(previous, previous_view);
        if (previous_view.count() != 0) {
          result.predecessor = previous_view.payload_value(previous_view.count() - 1);
        }
        previous->check_version_or_restart(previous_version);
      }
      if (next != nullptr) {
        Node next_view(true, false);
        load_view(next, next_view);
        if (next_view.count() != 0)
          result.successor = next_view.payload_value(0);
        next->check_version_or_restart(next_version);
      }
      return result;
    } catch (const RestartOperation&) {
#ifdef DV_TESTING
      count_restart(RestartPoint::NeighborSiblings);
#endif
      olc_backoff(retry);
    }
  }
}

void BTree::split_node(NodeWriteGuard node_guard, NodeWriteGuard parent_guard, const uint8_t* key,
                       unsigned key_length) {
  static_cast<void>(key);
  static_cast<void>(key_length);
  Node* node = node_guard.get();
  Node* parent = parent_guard.get();
  if (parent == nullptr) {
    parent = new Node(false, true, &accounting_);
    parent_guard = NodeWriteGuard(parent);
    parent->header().upper = node;
    root_.store(parent, std::memory_order_release);
  }

  const Node::SeparatorInfo separator = node->find_separator();
  std::array<uint8_t, Node::MAX_KEY_LENGTH> separator_bytes{};
  node->separator_key(separator_bytes.data(), separator);
  if (parent->request_space(parent->space_needed(separator.length))) {
    NodeWriteGuard previous_guard;
    Node* previous = nullptr;
    if (node->is_leaf()) {
      previous = node->control.previous_leaf.load(std::memory_order_relaxed);
      if (previous != nullptr) {
        // Do not wait for a sibling while holding the split node and
        // its parent. Validate and try-lock the sibling; contention
        // restarts the complete OLC operation and releases both
        // already-held guards during stack unwinding.
        const uint64_t previous_version = previous->read_version_or_restart();
        previous_guard = NodeWriteGuard(previous, previous_version);
      }
    }
    node->split(parent, separator.slot, separator_bytes.data(), separator.length, previous != nullptr);
    node_guard.release();
    previous_guard.release();
    parent_guard.release();
    return;
  }

  Node* full_parent = parent;
  node_guard.release();
  parent_guard.release();
  ensure_space(full_parent, separator_bytes.data(), separator.length);
}

void BTree::ensure_space(Node* target, const uint8_t* key, unsigned key_length) {
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* node = root_.load(std::memory_order_acquire);
      uint64_t version = node->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != node)
        throw RestartOperation{};
      Node* parent = nullptr;
      uint64_t parent_version = 0;

      while (node != target) {
        Node view(true, false);
        load_view(node, view);
        if (view.is_leaf()) {
          node->check_version_or_restart(version);
          return;  // target was already split off this search path
        }
        Node* child = view.lookup_inner(key, key_length);
        node->check_version_or_restart(version);
        const uint64_t child_version = child->read_version_or_restart();
        node->check_version_or_restart(version);
        parent = node;
        parent_version = version;
        node = child;
        version = child_version;
      }

      Node view(true, false);
      load_view(node, view);
      const bool already_fits = view.space_needed(key_length) <= view.free_space_after_compaction();
      node->check_version_or_restart(version);
      if (already_fits)
        return;

      NodeWriteGuard parent_guard;
      if (parent != nullptr)
        parent_guard = NodeWriteGuard(parent, parent_version);
      NodeWriteGuard node_guard(node, version);
      split_node(std::move(node_guard), std::move(parent_guard), key, key_length);
      return;
    } catch (const RestartOperation&) {
      olc_backoff(retry);
    }
  }
}

void BTree::insert(const uint8_t* key, unsigned key_length, void* payload) {
  if (payload == nullptr)
    throw std::invalid_argument("B-tree pointer payload must not be null");
  if (key_length > Node::MAX_KEY_LENGTH) {
    throw std::length_error("key exceeds the B-tree page limit");
  }
  for (uint64_t retry = 0;; ++retry) {
    try {
      Node* node = root_.load(std::memory_order_acquire);
      Node* parent = nullptr;
      uint64_t parent_version = 0;
      uint64_t version = node->read_version_or_restart();
      if (root_.load(std::memory_order_acquire) != node)
        throw RestartOperation{};
      Node view(true, false);

      for (;;) {
        load_view(node, view);
        if (view.is_leaf())
          break;
        Node* child = view.lookup_inner(key, key_length);
        node->check_version_or_restart(version);
        const uint64_t child_version = child->read_version_or_restart();
        node->check_version_or_restart(version);
        parent = node;
        parent_version = version;
        node = child;
        version = child_version;
      }

      bool found = false;
      view.lower_bound(key, key_length, found);
      if (found) {
        node->check_version_or_restart(version);
        throw DuplicateKeyError("duplicate key passed to basic B-tree engine");
      }

      const bool fits = view.space_needed(key_length) <= view.free_space_after_compaction();
      if (fits) {
#ifdef DV_TESTING
        if (consume_forced_restart(RestartPoint::InsertLeaf))
          throw RestartOperation{};
#endif
        NodeWriteGuard leaf_guard(node, version);
        if (!node->insert(key, key_length, payload)) {
          throw std::logic_error("validated leaf capacity changed while write-locked");
        }
        return;
      }

      NodeWriteGuard parent_guard;
      if (parent != nullptr)
        parent_guard = NodeWriteGuard(parent, parent_version);
      NodeWriteGuard node_guard(node, version);
      split_node(std::move(node_guard), std::move(parent_guard), key, key_length);
    } catch (const RestartOperation&) {
#ifdef DV_TESTING
      count_restart(RestartPoint::InsertLeaf);
#endif
      olc_backoff(retry);
    }
  }
}

bool BTree::for_each(const std::function<bool(const uint8_t*, unsigned, void*)>& callback) const {
  for (Node* leaf = first_leaf(); leaf != nullptr;) {
    for (uint64_t retry = 0;; ++retry) {
      try {
        const uint64_t version = leaf->read_version_or_restart();
        Node view(true, false);
        load_view(leaf, view);
        Node* next = leaf->control.next_leaf.load(std::memory_order_relaxed);
        leaf->check_version_or_restart(version);
        if (!view.visit(callback))
          return false;
        leaf = next;
        break;
      } catch (const RestartOperation&) {
        olc_backoff(retry);
      }
    }
  }
  return true;
}

}  // namespace hyrise::dv_tree::engine
