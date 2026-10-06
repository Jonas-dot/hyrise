#pragma once

// Adapted TUM btree-cpp page engine used as DV-Tree's private structural layer.
// See README.md in this directory for the takeover and modification boundary.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>

namespace hyrise::dv_tree::engine {

struct RestartOperation {};

struct DuplicateKeyError : std::logic_error {
  using std::logic_error::logic_error;
};

inline constexpr unsigned PAGE_SIZE = 4096;
inline constexpr unsigned HINT_COUNT = 16;
inline constexpr unsigned PAYLOAD_SIZE = sizeof(void*);

struct Node;
struct PageBody;

struct NodeAccounting {
  std::atomic<std::size_t> nodes{0};
  std::atomic<std::size_t> inner_nodes{0};
  std::atomic<std::size_t> leaf_nodes{0};
};

// This object is never copied by page compaction or splitting. Its 64-bit OLC
// word uses the high byte as lock state and the low 56 bits as the version.
// Readers consume immutable published page images and validate this version.
struct NodeControl {
  static constexpr uint64_t VERSION_MASK = (uint64_t{1} << 56) - 1;
  static constexpr uint64_t LOCKED_STATE = uint64_t{253} << 56;

  std::atomic<uint64_t> version_lock{0};
  std::atomic<Node*> previous_leaf{nullptr};
  std::atomic<Node*> next_leaf{nullptr};
  std::shared_ptr<const PageBody> published_page;

  NodeControl() = default;
  NodeControl(const NodeControl&) = delete;
  NodeControl& operator=(const NodeControl&) = delete;
};

struct FenceKeySlot {
  uint16_t offset = 0;
  uint16_t length = 0;
};

struct PageHeader {
  explicit PageHeader(bool leaf = true) : is_leaf(leaf), data_offset(PAGE_SIZE) {}

  bool is_leaf = true;
  uint16_t count = 0;
  uint16_t space_used = 0;
  uint16_t data_offset = PAGE_SIZE;
  Node* upper = nullptr;
  FenceKeySlot lower_fence;
  FenceKeySlot upper_fence;
  uint16_t prefix_length = 0;
  uint32_t hints[HINT_COUNT]{};
};

#pragma pack(push, 1)

struct Slot {
  uint16_t offset;
  uint16_t key_length;
  uint32_t head;
};

#pragma pack(pop)

static_assert(sizeof(Slot) == 8, "a fixed pointer payload needs no per-slot payload length");

// Exactly one pageable/copyable node image. No atomics or sibling pointers live
// here; copying this byte array cannot overwrite synchronization state.
struct alignas(PageHeader) PageBody {
  uint8_t bytes[PAGE_SIZE]{};
};

static_assert(sizeof(PageBody) == PAGE_SIZE);

struct Node {
  struct SeparatorInfo {
    unsigned length;
    unsigned slot;
    bool truncated;
  };

  struct Neighbors {
    void* predecessor = nullptr;
    void* successor = nullptr;
  };

  // Port of btree-cpp's structural /3 bound, specialized to a fixed pointer
  // payload. This guarantees at least three entries fit in an empty page.
  static constexpr unsigned MAX_KEY_LENGTH = (PAGE_SIZE - sizeof(PageHeader) - 2 * sizeof(Slot)) / 3 - PAYLOAD_SIZE;

  explicit Node(bool leaf, bool publish_initial = true, NodeAccounting* accounting = nullptr);
  ~Node();
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  NodeControl control;
  PageBody page;

  PageHeader& header();
  const PageHeader& header() const;
  Slot* slots();
  const Slot* slots() const;
  uint8_t* key(unsigned slot);
  const uint8_t* key(unsigned slot) const;
  uint8_t* payload_bytes(unsigned slot);
  const uint8_t* payload_bytes(unsigned slot) const;
  void* payload_value(unsigned slot) const;
  Node* child(unsigned slot) const;
  uint8_t* lower_fence_bytes();
  uint8_t* upper_fence_bytes();
  const uint8_t* prefix() const;

  bool is_leaf() const {
    return header().is_leaf;
  }

  unsigned count() const {
    return header().count;
  }

  unsigned prefix_length() const {
    return header().prefix_length;
  }

  Node* upper() const {
    return header().upper;
  }

  uint64_t read_version_or_restart() const;
  void check_version_or_restart(uint64_t version) const;
  bool try_write_lock(uint64_t expected);
  void write_lock();
  void write_unlock();
  std::shared_ptr<const PageBody> published_page() const;
  static std::shared_ptr<PageBody> allocate_page_image();
  void publish_page();
  void publish_page(std::shared_ptr<PageBody> image) noexcept;

#ifdef DV_TESTING
  static void fail_next_page_publication_for_test();
#endif

  unsigned free_space() const;
  unsigned free_space_after_compaction() const;
  unsigned space_needed(unsigned key_length) const;
  bool request_space(unsigned bytes_needed);

  unsigned lower_bound(const uint8_t* search_key, unsigned key_length, bool& found) const;
  unsigned lower_bound(const uint8_t* search_key, unsigned key_length) const;
  Node* lookup_inner(const uint8_t* search_key, unsigned key_length) const;

  bool insert(const uint8_t* key, unsigned key_length, void* payload);
  bool insert_child(const uint8_t* key, unsigned key_length, Node* child);
  void store(unsigned slot, const uint8_t* key, unsigned key_length, void* payload);

  void make_hints();
  void update_hints(unsigned inserted_slot);
  void search_hints(uint32_t key_head, unsigned& lower, unsigned& upper) const;
  void compactify();

  void insert_fence(FenceKeySlot& destination, const uint8_t* key, unsigned key_length);
  void set_fences(const uint8_t* lower, unsigned lower_length, const uint8_t* upper, unsigned upper_length);
  void copy_entry(unsigned source_slot, Node* destination, unsigned destination_slot) const;
  void copy_range(Node* destination, unsigned destination_slot, unsigned source_slot, unsigned source_count) const;

  unsigned common_prefix(unsigned left_slot, unsigned right_slot) const;
  SeparatorInfo find_separator() const;
  void restore_key(uint8_t* destination, unsigned length, unsigned slot) const;
  void separator_key(uint8_t* destination, SeparatorInfo separator) const;
  bool split_with_parent(Node* parent);
  void split(Node* parent, unsigned separator_slot, const uint8_t* separator_key, unsigned separator_length,
             bool previous_leaf_is_locked = false);

  bool visit(const std::function<bool(const uint8_t*, unsigned, void*)>& callback) const;

 private:
  NodeAccounting* accounting_ = nullptr;
  bool accounted_leaf_ = false;
};

class NodeWriteGuard {
 public:
  NodeWriteGuard() = default;
  explicit NodeWriteGuard(Node* node);
  NodeWriteGuard(Node* node, uint64_t expected);
  ~NodeWriteGuard();
  NodeWriteGuard(const NodeWriteGuard&) = delete;
  NodeWriteGuard& operator=(const NodeWriteGuard&) = delete;
  NodeWriteGuard(NodeWriteGuard&& other) noexcept;
  NodeWriteGuard& operator=(NodeWriteGuard&& other) noexcept;

  Node* get() const {
    return node_;
  }

  Node* operator->() const {
    return node_;
  }

  explicit operator bool() const {
    return node_ != nullptr;
  }

  void release() noexcept;

 private:
  // Allocate before taking the page latch. Once a mutation starts, publication
  // and unlock are therefore allocation-free and cannot throw.
  std::shared_ptr<PageBody> publication_buffer_;
  Node* node_ = nullptr;
};

class BTree {
 public:
  BTree();
  ~BTree();
  BTree(const BTree&) = delete;
  BTree& operator=(const BTree&) = delete;

  void* lookup(const uint8_t* key, unsigned key_length) const;
  void insert(const uint8_t* key, unsigned key_length, void* payload);
  Node::Neighbors neighbors(const uint8_t* absent_key, unsigned key_length) const;
  bool for_each(const std::function<bool(const uint8_t*, unsigned, void*)>& callback) const;

  Node* root() const {
    return root_.load(std::memory_order_acquire);
  }

  Node* find_leaf(const uint8_t* key, unsigned key_length) const;
  Node* first_leaf() const;

  std::size_t node_count() const {
    return accounting_.nodes.load(std::memory_order_relaxed);
  }

  std::size_t inner_node_count() const {
    return accounting_.inner_nodes.load(std::memory_order_relaxed);
  }

  std::size_t leaf_node_count() const {
    return accounting_.leaf_nodes.load(std::memory_order_relaxed);
  }

#ifdef DV_TESTING
  enum class RestartPoint { LookupLeaf, NeighborSiblings, InsertLeaf };

  void force_restart_once(RestartPoint point);
  uint64_t restart_count(RestartPoint point) const;
#endif

 private:
  void split_node(NodeWriteGuard node, NodeWriteGuard parent, const uint8_t* key, unsigned key_length);
  void ensure_space(Node* target, const uint8_t* key, unsigned key_length);
  static void destroy(Node* node);

#ifdef DV_TESTING
  bool consume_forced_restart(RestartPoint point) const;
  void count_restart(RestartPoint point) const;

  mutable std::atomic<bool> force_lookup_restart_{false};
  mutable std::atomic<bool> force_neighbor_restart_{false};
  mutable std::atomic<bool> force_insert_restart_{false};
  mutable std::atomic<uint64_t> lookup_restarts_{0};
  mutable std::atomic<uint64_t> neighbor_restarts_{0};
  mutable std::atomic<uint64_t> insert_restarts_{0};
#endif

  NodeAccounting accounting_;
  std::atomic<Node*> root_;
};

}  // namespace hyrise::dv_tree::engine
