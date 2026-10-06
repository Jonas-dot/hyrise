#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "public_api_smoke.hpp"
#include "test_internal.hpp"

using namespace hyrise::dv_tree;  // NOLINT(build/namespaces)
using hyrise::CommitID;
using hyrise::INITIAL_COMMIT_ID;
using hyrise::MAX_COMMIT_ID;
using hyrise::UNSET_COMMIT_ID;

static int checks = 0;
static int failures = 0;

#define CHECK(condition)                                                           \
  do {                                                                             \
    ++checks;                                                                      \
    if (!(condition)) {                                                            \
      ++failures;                                                                  \
      std::cerr << "FAIL " << __FILE__ << ':' << __LINE__ << ": " #condition "\n"; \
    }                                                                              \
  } while (false)

static void test_olc_version_wrap_preserves_lock_state() {
  engine::Node node(true);
  node.control.version_lock.store(engine::NodeControl::VERSION_MASK, std::memory_order_relaxed);

  const bool locked = node.try_write_lock(engine::NodeControl::VERSION_MASK);
  CHECK(locked);
  if (!locked)
    return;
  CHECK((node.control.version_lock.load(std::memory_order_relaxed) & ~engine::NodeControl::VERSION_MASK) ==
        engine::NodeControl::LOCKED_STATE);

  node.write_unlock();
  CHECK(node.control.version_lock.load(std::memory_order_acquire) == 0);
  CHECK(node.read_version_or_restart() == 0);
}

static void test_hyrise_commit_id_boundaries() {
  CHECK(to_internal_snapshot_id(UNSET_COMMIT_ID) == INVALID_INTERNAL_COMMIT_ID);
  CHECK(to_internal_commit_id(INITIAL_COMMIT_ID) == 1);
  CHECK(to_hyrise_commit_id(INVALID_INTERNAL_COMMIT_ID) == UNSET_COMMIT_ID);

  bool unset_commit_rejected = false;
  try {
    static_cast<void>(to_internal_commit_id(UNSET_COMMIT_ID));
  } catch (const InvalidDVCommitID&) {
    unset_commit_rejected = true;
  }
  CHECK(unset_commit_rejected);

  bool reserved_snapshot_rejected = false;
  try {
    static_cast<void>(to_internal_snapshot_id(MAX_COMMIT_ID));
  } catch (const InvalidDVCommitID&) {
    reserved_snapshot_rejected = true;
  }
  CHECK(reserved_snapshot_rejected);

  bool oversized_internal_rejected = false;
  try {
    static_cast<void>(to_hyrise_commit_id(MAX_VALID_INTERNAL_COMMIT_ID + 1));
  } catch (const InvalidDVCommitID&) {
    oversized_internal_rejected = true;
  }
  CHECK(oversized_internal_rejected);

  const auto last_usable_raw = static_cast<CommitID::base_type>(MAX_COMMIT_ID) - 1;
  const auto before_last_usable = CommitID{last_usable_raw - 1};
  const auto last_usable = CommitID{last_usable_raw};
  DVTree near_limit{DependencyKind::FD, 8, before_last_usable};
  Transaction transaction;
  transaction.insert(make_normalized_key("cid-boundary"), make_normalized_key(1));
  CHECK(near_limit.apply_commit(transaction) == last_usable);
  CHECK(near_limit.visible_commit_id() == last_usable);
  CHECK(near_limit.holds_exact(last_usable));

  bool allocator_exhaustion_rejected = false;
  try {
    static_cast<void>(near_limit.begin_commit());
  } catch (const CommitOrderViolation&) {
    allocator_exhaustion_rejected = true;
  }
  CHECK(allocator_exhaustion_rejected);

  bool reserved_baseline_rejected = false;
  try {
    DVTree invalid_baseline{DependencyKind::FD, 8, MAX_COMMIT_ID};
  } catch (const InvalidDVCommitID&) {
    reserved_baseline_rejected = true;
  }
  CHECK(reserved_baseline_rejected);
}

static void test_external_sparse_commit_ids_and_registration_frontier() {
  DVTree index{DependencyKind::FD, 16};
  const std::string lhs = make_normalized_key("sparse");
  const std::string rhs_a = make_normalized_key("a");
  const std::string rhs_b = make_normalized_key("b");

  auto cid_10 = index.begin_commit(CommitID{10});
  cid_10.insert(lhs, rhs_a);
  cid_10.seal();
  CHECK(index.visible_commit_id() == UNSET_COMMIT_ID);
  CHECK(!index.effects_installed_for_test(CommitID{10}));

  index.advance_registration_frontier(CommitID{10});
  cid_10.wait_until_applied();
  CHECK(index.visible_commit_id() == UNSET_COMMIT_ID);
  index.advance_visibility_frontier(CommitID{10});
  cid_10.wait_until_visible();
  CHECK(index.visible_commit_id() == CommitID{10});
  CHECK(index.holds_exact(CommitID{10}));

  // CIDs 11 through 20 did not touch this tree. The frontier makes those gaps
  // visible without manufacturing history records for them.
  index.advance_registration_frontier(CommitID{20});
  CHECK(index.visible_commit_id() == CommitID{10});
  index.advance_visibility_frontier(CommitID{20});
  CHECK(index.visible_commit_id() == CommitID{20});
  CHECK(index.holds_exact(CommitID{15}));

  auto cid_25 = index.begin_commit(CommitID{25});
  cid_25.insert(lhs, rhs_b);
  cid_25.seal();

  // A frontier below CID 25 proves the gap through 24, but CID 25 itself must
  // remain parked because a lower registration could still arrive.
  index.advance_registration_frontier(CommitID{24});
  index.advance_visibility_frontier(CommitID{24});
  CHECK(index.visible_commit_id() == CommitID{24});
  CHECK(!index.effects_installed_for_test(CommitID{25}));
  CHECK(index.holds_exact(CommitID{24}));

  index.advance_registration_frontier(CommitID{25});
  cid_25.wait_until_applied();
  CHECK(index.visible_commit_id() == CommitID{24});
  index.advance_visibility_frontier(CommitID{25});
  cid_25.wait_until_visible();
  CHECK(index.visible_commit_id() == CommitID{25});
  CHECK(index.violations_exact(CommitID{24}) == 0);
  CHECK(index.violations_exact(CommitID{25}) == 1);

  auto cid_100 = index.begin_commit(CommitID{100});
  cid_100.remove(lhs, rhs_b);
  cid_100.seal();
  index.advance_registration_frontier(CommitID{100});
  cid_100.wait_until_applied();
  index.advance_visibility_frontier(CommitID{100});
  cid_100.wait_until_visible();
  CHECK(index.visible_commit_id() == CommitID{100});
  CHECK(index.violations_exact(CommitID{99}) == 1);
  CHECK(index.violations_exact(CommitID{100}) == 0);
  CHECK(index.history().size() == 2);  // only the non-zero changes at 25 and 100
}

static void test_external_out_of_order_registration_and_rejections() {
  DVTree index{DependencyKind::FD, 16};
  const std::string lhs = make_normalized_key("out-of-order");

  // Registration may arrive out of order while the frontier is still open.
  auto higher = index.begin_commit(CommitID{40});
  higher.insert(lhs, make_normalized_key("higher"));
  higher.seal();
  auto lower = index.begin_commit(CommitID{35});
  lower.insert(lhs, make_normalized_key("lower"));
  lower.seal();

  bool duplicate_rejected = false;
  try {
    static_cast<void>(index.begin_commit(CommitID{40}));
  } catch (const CommitOrderViolation&) {
    duplicate_rejected = true;
  }
  CHECK(duplicate_rejected);

  index.advance_registration_frontier(CommitID{40});
  lower.wait_until_applied();
  higher.wait_until_applied();
  CHECK(index.visible_commit_id() == UNSET_COMMIT_ID);
  index.advance_visibility_frontier(CommitID{40});
  lower.wait_until_visible();
  higher.wait_until_visible();
  CHECK(index.visible_commit_id() == CommitID{40});
  CHECK(index.violations_exact(CommitID{35}) == 0);
  CHECK(index.violations_exact(CommitID{40}) == 1);

  bool late_lower_rejected = false;
  try {
    static_cast<void>(index.begin_commit(CommitID{38}));
  } catch (const CommitOrderViolation&) {
    late_lower_rejected = true;
  }
  CHECK(late_lower_rejected);

  bool backwards_frontier_rejected = false;
  try {
    index.advance_registration_frontier(CommitID{39});
  } catch (const CommitOrderViolation&) {
    backwards_frontier_rejected = true;
  }
  CHECK(backwards_frontier_rejected);

  bool allocator_mixing_rejected = false;
  try {
    static_cast<void>(index.begin_commit());
  } catch (const CommitOrderViolation&) {
    allocator_mixing_rejected = true;
  }
  CHECK(allocator_mixing_rejected);

  auto aborted = index.begin_commit(CommitID{50});
  aborted.abort();
  index.advance_registration_frontier(CommitID{50});
  aborted.wait_until_applied();
  CHECK(index.visible_commit_id() == CommitID{40});
  index.advance_visibility_frontier(CommitID{50});
  aborted.wait_until_visible();
  CHECK(index.visible_commit_id() == CommitID{50});
  CHECK(index.violations_exact(CommitID{50}) == 1);
}

static void test_stale_split_prefix_forces_restart() {
  engine::Node stale_right_half(true, false);
  const uint8_t lower[] = {'m', 'i', 'd'};
  const uint8_t upper[] = {'m', 'a', 'x'};
  stale_right_half.set_fences(lower, sizeof(lower), upper, sizeof(upper));
  const uint8_t old_parent_key[] = {'a'};
  bool found = false;
  bool restarted = false;
  try {
    static_cast<void>(stale_right_half.lower_bound(old_parent_key, sizeof(old_parent_key), found));
  } catch (const engine::RestartOperation&) {
    restarted = true;
  }
  CHECK(restarted);
}

static void test_failed_page_publication_preserves_reader_image() {
  engine::Node node(true);
  const auto before = node.published_page();
  CHECK(before != nullptr);
  if (!before)
    return;

  constexpr std::size_t probe = engine::PAGE_SIZE - 1;
  const uint8_t published_byte = before->bytes[probe];
  node.page.bytes[probe] ^= 0x5Au;

  engine::Node::fail_next_page_publication_for_test();
  bool allocation_failed = false;
  try {
    node.publish_page();
  } catch (const std::bad_alloc&) {
    allocation_failed = true;
  }
  CHECK(allocation_failed);

  const auto after_failure = node.published_page();
  CHECK(after_failure.get() == before.get());
  CHECK(after_failure->bytes[probe] == published_byte);

  node.publish_page();
  const auto after_success = node.published_page();
  CHECK(after_success.get() != before.get());
  CHECK(after_success->bytes[probe] == node.page.bytes[probe]);
}

static void test_page_image_allocation_fails_before_write_lock() {
  engine::Node node(true);
  const auto before = node.published_page();
  CHECK(before != nullptr);

  engine::Node::fail_next_page_publication_for_test();
  bool allocation_failed = false;
  try {
    auto guard = engine::NodeWriteGuard{&node};
    static_cast<void>(guard);
  } catch (const std::bad_alloc&) {
    allocation_failed = true;
  }

  CHECK(allocation_failed);
  CHECK(node.read_version_or_restart() == 0);
  CHECK(node.published_page() == before);

  // A later writer can still acquire and release the node, proving the failed
  // preallocation did not leave the physical page latch held.
  {
    auto guard = engine::NodeWriteGuard{&node};
    node.page.bytes[engine::PAGE_SIZE - 1] ^= 0x5Au;
  }
  CHECK(node.read_version_or_restart() == 1);
  CHECK(node.published_page() != before);
}

static float float_from_bits(uint32_t bits) {
  float value = 0;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

static double double_from_bits(uint64_t bits) {
  double value = 0;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

static void test_normalization_order_and_boundaries() {
  CHECK(make_normalized_key(-2) < make_normalized_key(-1));
  CHECK(make_normalized_key(-1) < make_normalized_key(0));
  CHECK(make_normalized_key(0) < make_normalized_key(100));
  CHECK(make_normalized_key(std::optional<int32_t>{-100}) < make_normalized_key(std::optional<int32_t>{}));

  const std::string left = make_normalized_key("a", "bc");
  const std::string right = make_normalized_key("ab", "c");
  CHECK(left != right);  // variable columns are self-delimiting
  CHECK(make_normalized_key("tenant", 4, "a") < make_normalized_key("tenant", 4, "b"));
  CHECK(make_normalized_key("tenant", 4) < make_normalized_key("tenant", 5));

  CHECK(make_normalized_key(int64_t{-9}) < make_normalized_key(int64_t{12}));
  CHECK(make_normalized_key(-4.5f) < make_normalized_key(-0.25f));
  CHECK(make_normalized_key(-0.25f) < make_normalized_key(0.0f));
  CHECK(make_normalized_key(0.0f) < make_normalized_key(3.5f));
  CHECK(make_normalized_key(-0.0f) == make_normalized_key(0.0f));
  CHECK(make_normalized_key(-0.0) == make_normalized_key(0.0));

  const std::string embedded_null("a\0b", 3);
  const std::string embedded_ff("a\xFF", 2);
  CHECK(make_normalized_key(embedded_null, "tail") != make_normalized_key(std::string("a"), std::string("b\0tail", 6)));
  CHECK(make_normalized_key(std::string("a")) < make_normalized_key(embedded_null));
  CHECK(make_normalized_key(embedded_null) < make_normalized_key(embedded_ff));
}

static void test_duckdb_nan_and_null_normalization_policy() {
  const float float_nan_a = std::numeric_limits<float>::quiet_NaN();
  const float float_nan_b = float_from_bits(0x7FC01234u);
  const float float_nan_negative = float_from_bits(0xFFC05678u);
  const double double_nan_a = std::numeric_limits<double>::quiet_NaN();
  const double double_nan_b = double_from_bits(0x7FF8000000001234ULL);
  const double double_nan_negative = double_from_bits(0xFFF8000000005678ULL);

  CHECK(make_normalized_key(float_nan_a) == make_normalized_key(float_nan_b));
  CHECK(make_normalized_key(float_nan_a) == make_normalized_key(float_nan_negative));
  CHECK(make_normalized_key(double_nan_a) == make_normalized_key(double_nan_b));
  CHECK(make_normalized_key(double_nan_a) == make_normalized_key(double_nan_negative));

  CHECK(make_normalized_key(-std::numeric_limits<float>::infinity()) <
        make_normalized_key(-std::numeric_limits<float>::max()));
  CHECK(make_normalized_key(std::numeric_limits<float>::max()) <
        make_normalized_key(std::numeric_limits<float>::infinity()));
  CHECK(make_normalized_key(std::numeric_limits<float>::infinity()) < make_normalized_key(float_nan_a));
  CHECK(make_normalized_key(-std::numeric_limits<double>::infinity()) <
        make_normalized_key(-std::numeric_limits<double>::max()));
  CHECK(make_normalized_key(std::numeric_limits<double>::max()) <
        make_normalized_key(std::numeric_limits<double>::infinity()));
  CHECK(make_normalized_key(std::numeric_limits<double>::infinity()) < make_normalized_key(double_nan_a));

  std::string encoded;
  normalize_float(-std::numeric_limits<float>::infinity(), encoded);
  CHECK(encoded == std::string("\x00\x00\x00\x00", 4));
  encoded.clear();
  normalize_float(std::numeric_limits<float>::infinity(), encoded);
  CHECK(encoded == std::string("\xFF\xFF\xFF\xFE", 4));
  encoded.clear();
  normalize_float(float_nan_b, encoded);
  CHECK(encoded == std::string("\xFF\xFF\xFF\xFF", 4));
  encoded.clear();
  normalize_double(-std::numeric_limits<double>::infinity(), encoded);
  CHECK(encoded == std::string(8, '\0'));
  encoded.clear();
  normalize_double(std::numeric_limits<double>::infinity(), encoded);
  CHECK(encoded == std::string("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE", 8));
  encoded.clear();
  normalize_double(double_nan_b, encoded);
  CHECK(encoded == std::string("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8));

  const auto null_int = make_normalized_key(std::optional<int32_t>{});
  const auto valid_int = make_normalized_key(std::optional<int32_t>{7});
  CHECK(null_int.size() == 1);
  CHECK(null_int[0] == NORMALIZED_NULL_MARKER);
  CHECK(valid_int[0] == NORMALIZED_VALID_MARKER);
  CHECK(valid_int < null_int);  // fixed ASC NULLS LAST

  const auto composite_valid = make_normalized_key("tenant", std::optional<int32_t>{7}, "tail");
  const auto composite_null = make_normalized_key("tenant", std::optional<int32_t>{}, "tail");
  CHECK(composite_valid < composite_null);
  CHECK(composite_null == make_normalized_key("tenant", std::optional<int32_t>{}, "tail"));
}

static void test_duckdb_nan_and_null_dependency_semantics() {
  const float nan_a = float_from_bits(0x7FC00001u);
  const float nan_b = float_from_bits(0xFFC01234u);
  const auto nullable_lhs = make_normalized_key("tenant", std::optional<int32_t>{});
  const auto nan_rhs_a = make_normalized_key(nan_a);
  const auto nan_rhs_b = make_normalized_key(nan_b);
  const auto infinity_rhs = make_normalized_key(std::numeric_limits<float>::infinity());

  DVTree fd_nan(DependencyKind::FD);
  Transaction duplicate_nans;
  duplicate_nans.insert(nullable_lhs, nan_rhs_a);
  duplicate_nans.insert(nullable_lhs, nan_rhs_b);
  CHECK(fd_nan.apply_commit(duplicate_nans) == 1);
  CHECK(fd_nan.holds());  // all NaN payloads are one distinct RHS value
  const auto nan_snapshot = fd_nan.snapshot_entry(nullable_lhs);
  CHECK(nan_snapshot.has_value());
  CHECK(nan_snapshot->rhs_counts.size() == 1);
  CHECK(nan_snapshot->rhs_counts.front().second == 2);

  Transaction add_infinity;
  add_infinity.insert(nullable_lhs, infinity_rhs);
  CHECK(fd_nan.apply_commit(add_infinity) == 2);
  CHECK(fd_nan.violations() == 1);  // +Infinity and NaN are distinct

  DVTree fd_null(DependencyKind::FD);
  const auto ordinary_lhs = make_normalized_key(7);
  const auto null_rhs = make_normalized_key(std::optional<int32_t>{});
  const auto ten_rhs = make_normalized_key(std::optional<int32_t>{10});
  Transaction duplicate_nulls;
  duplicate_nulls.insert(ordinary_lhs, null_rhs);
  duplicate_nulls.insert(ordinary_lhs, null_rhs);
  CHECK(fd_null.apply_commit(duplicate_nulls) == 1);
  CHECK(fd_null.holds());  // NULL IS NOT DISTINCT FROM NULL
  CHECK(fd_null.snapshot_entry(ordinary_lhs)->rhs_counts.front().second == 2);
  Transaction add_non_null;
  add_non_null.insert(ordinary_lhs, ten_rhs);
  CHECK(fd_null.apply_commit(add_non_null) == 2);
  CHECK(fd_null.violations() == 1);  // NULL is distinct from every non-NULL value

  DVTree od_null(DependencyKind::OD);
  const auto lhs_one = make_normalized_key(1);
  const auto lhs_two = make_normalized_key(2);
  Transaction null_then_value;
  null_then_value.insert(lhs_one, null_rhs);
  null_then_value.insert(lhs_two, ten_rhs);
  CHECK(od_null.apply_commit(null_then_value) == 1);
  CHECK(od_null.violations() == 1);  // NULLS LAST: NULL > 10
  CHECK(od_null.snapshot_entry(lhs_one)->neighbor_violation == 1);
  Transaction repair_null_order;
  repair_null_order.update(lhs_one, null_rhs, make_normalized_key(5));
  CHECK(od_null.apply_commit(repair_null_order) == 2);
  CHECK(od_null.holds());

  DVTree od_nan(DependencyKind::OD);
  Transaction nan_then_infinity;
  nan_then_infinity.insert(lhs_one, nan_rhs_a);
  nan_then_infinity.insert(lhs_two, infinity_rhs);
  CHECK(od_nan.apply_commit(nan_then_infinity) == 1);
  CHECK(od_nan.violations() == 1);  // DuckDB order: NaN > +Infinity
}

static void test_upstream_engine_with_variable_composite_keys() {
  BTreeCppAdapter tree;
  constexpr int count = 5000;
  std::vector<std::string> keys;
  std::vector<int> payloads(count);
  keys.reserve(count);
  for (int i = 0; i < count; ++i) {
    keys.push_back(make_normalized_key("shared-tenant-prefix", i, std::string(i % 11, 'x')));
    payloads[i] = i;
  }

  std::vector<int> order(count);
  for (int i = 0; i < count; ++i)
    order[i] = i;
  std::mt19937 generator(0xD7B7u);
  std::shuffle(order.begin(), order.end(), generator);
  for (int index : order)
    tree.insert_new(keys[index], &payloads[index]);

  for (int i = 0; i < count; ++i) {
    CHECK(tree.find(keys[i]) == &payloads[i]);
  }
  CHECK(tree.find(make_normalized_key("missing", 7)) == nullptr);

  std::vector<std::string> scanned;
  tree.for_each([&](std::string_view key, void*) {
    scanned.emplace_back(key);
    return true;
  });
  CHECK(scanned.size() == keys.size());
  CHECK(std::is_sorted(scanned.begin(), scanned.end(), ByteStringLess{}));
  auto expected = keys;
  std::sort(expected.begin(), expected.end(), ByteStringLess{});
  CHECK(scanned == expected);  // full keys survive prefix changes and multi-level splits

  const auto engine_stats = tree.stats();
  CHECK(engine_stats.entries == count);
  CHECK(engine_stats.nodes > 1);
  CHECK(engine_stats.inner_nodes > 0);
  CHECK(engine_stats.height > 1);
  CHECK(engine_stats.max_prefix_length > 0);
  CHECK(engine_stats.linked_leaf_nodes == engine_stats.leaf_nodes);
  CHECK(engine_stats.leaf_chain_valid);
}

static void test_engine_neighbors_across_pages() {
  BTreeCppAdapter tree;
  std::vector<int> values(1500);
  for (int i = 0; i < 1500; ++i) {
    values[i] = i;
    tree.insert_new(make_normalized_key("account", i * 2), &values[i]);
  }
  const auto adjacent = tree.neighbors(make_normalized_key("account", 1501));
  CHECK(adjacent.predecessor == &values[750]);  // key 1500
  CHECK(adjacent.successor == &values[751]);    // key 1502

  const auto before_first = tree.neighbors(make_normalized_key("account", -1));
  CHECK(before_first.predecessor == nullptr);
  CHECK(before_first.successor == &values.front());

  const auto after_last = tree.neighbors(make_normalized_key("account", 4000));
  CHECK(after_last.predecessor == &values.back());
  CHECK(after_last.successor == nullptr);
}

static void test_page_layout_and_control_are_separate() {
  CHECK(sizeof(engine::PageBody) == engine::PAGE_SIZE);
  CHECK(sizeof(engine::Slot) == 8);
  CHECK(engine::PAYLOAD_SIZE == sizeof(void*));
  CHECK(sizeof(engine::Node) > sizeof(engine::PageBody));

  engine::BTree tree;
  std::vector<int> values(2000);
  std::vector<std::string> keys;
  keys.reserve(values.size());
  for (int i = 0; i < 64; ++i) {
    keys.push_back(make_normalized_key("control-preservation", i));
    values[i] = i;
    tree.insert(reinterpret_cast<const uint8_t*>(keys.back().data()), static_cast<unsigned>(keys.back().size()),
                &values[i]);
  }

  // Compaction copies only PageBody. The OLC word in NodeControl must
  // remain attached to the node object and retain its value.
  engine::Node* original_leaf = tree.root();
  constexpr uint64_t marker = 0x000056789ABCDEF0ULL;
  original_leaf->control.version_lock.store(marker);
  original_leaf->compactify();
  CHECK(original_leaf->control.version_lock.load() == marker);

  // Force repeated leaf and inner splits. The original leaf becomes a right
  // half but is never replaced or copied as a complete Node.
  for (int i = 64; i < static_cast<int>(values.size()); ++i) {
    keys.push_back(make_normalized_key("control-preservation", i));
    values[i] = i;
    tree.insert(reinterpret_cast<const uint8_t*>(keys.back().data()), static_cast<unsigned>(keys.back().size()),
                &values[i]);
  }
  const uint64_t final_version = original_leaf->control.version_lock.load();
  CHECK(final_version > marker);
  CHECK((final_version & ~engine::NodeControl::VERSION_MASK) == 0);
  CHECK(original_leaf->control.previous_leaf != nullptr);

  std::size_t leaf_count = 0;
  engine::Node* previous = nullptr;
  for (engine::Node* leaf = tree.first_leaf(); leaf != nullptr; leaf = leaf->control.next_leaf) {
    CHECK(leaf->control.previous_leaf == previous);
    previous = leaf;
    ++leaf_count;
  }
  CHECK(leaf_count > 1);
}

static void test_oversized_key_fails_before_upstream_assert() {
  BTreeCppAdapter tree;
  int payload = 1;
  bool threw = false;
  try {
    tree.insert_new(std::string(BTreeCppAdapter::max_key_length() + 1, 'z'), &payload);
  } catch (const BTreeCppAdapter::KeyTooLong&) {
    threw = true;
  }
  CHECK(threw);

  BTreeCppAdapter boundary_tree;
  std::string largest(BTreeCppAdapter::max_key_length(), 'k');
  boundary_tree.insert_new(largest, &payload);
  CHECK(boundary_tree.find(largest) == &payload);

  bool rejected_null = false;
  try {
    boundary_tree.insert_new("non-null-payload-contract", nullptr);
  } catch (const std::invalid_argument&) {
    rejected_null = true;
  }
  CHECK(rejected_null);
}

static void test_fd_metadata_is_out_of_line() {
  DVTree index(DependencyKind::FD);
  const auto lhs1 = make_normalized_key("customer", 1);
  const auto lhs2 = make_normalized_key("customer", 2);
  const auto rhs10 = make_normalized_key(10);
  const auto rhs11 = make_normalized_key(11);
  const auto rhs20 = make_normalized_key(20);

  Transaction seed;
  seed.insert(lhs2, rhs20);
  seed.insert(lhs1, rhs10);
  seed.insert(lhs1, rhs10);  // duplicate row, not a distinct-RHS violation
  CHECK(index.apply_commit(seed) == 1);
  CHECK(index.holds());
  CHECK(index.find(lhs1)->rhs_counts.begin()->second == 2);

  Transaction violate;
  violate.insert(lhs1, rhs11);
  CHECK(index.apply_commit(violate) == 2);
  CHECK(!index.holds());
  CHECK(index.violations() == 1);
  CHECK(index.find(lhs1)->local_violations == 1);
  CHECK(index.first_entry() == index.find(lhs1));
  CHECK(index.first_entry()->right == index.find(lhs2));

  // The old Hyrise layout could not represent this determinant group because
  // it serialized every distinct RHS into one 4 KiB leaf payload. Both the
  // distinct-value count and an individual RHS may exceed a page here.
  constexpr int additional_distinct_rhs = 4096;
  Transaction grow;
  for (int value = 0; value < additional_distinct_rhs; ++value) {
    grow.insert(lhs1, make_normalized_key(100000 + value));
  }
  grow.insert(lhs1, make_normalized_key(std::string(16 * 1024, 'r')));
  CHECK(index.apply_commit(grow) == 3);
  CHECK(index.find(lhs1)->rhs_counts.size() == static_cast<std::size_t>(additional_distinct_rhs + 3));
  CHECK(index.find(lhs1)->local_violations == additional_distinct_rhs + 2);
  CHECK(index.violations() == additional_distinct_rhs + 2);
}

static void test_memory_statistics_accounting() {
  DVTree index(DependencyKind::FD, 7);
  const auto empty = index.memory_statistics();
  CHECK(empty.btree_inner_pages == 0);
  CHECK(empty.btree_leaf_pages == 1);
  CHECK(empty.btree_node_bytes >= 4096);
  CHECK(empty.btree_published_image_bytes == 4096);
  CHECK(empty.dependency_entries == 0);
  CHECK(empty.history_soft_capacity == 7);
  CHECK(empty.total_accounted_bytes > empty.tree_object_bytes);

  constexpr int key_count = 400;
  Transaction seed;
  for (int key = 0; key < key_count; ++key) {
    seed.insert(make_normalized_key("memory", key), make_normalized_key(key));
  }
  CHECK(index.apply_commit(seed) == 1);

  Transaction violation;
  violation.insert(make_normalized_key("memory", 0), make_normalized_key(9999));
  CHECK(index.apply_commit(violation) == 2);
  const auto populated = index.memory_statistics();
  CHECK(populated.btree_leaf_pages > 1);
  CHECK(populated.btree_inner_pages >= 1);
  CHECK(populated.dependency_entries == key_count);
  CHECK(populated.distinct_rhs_values == key_count + 1);
  CHECK(populated.row_multiplicity == key_count + 1);
  CHECK(populated.rhs_allocations == key_count + 1);
  CHECK(populated.rhs_allocated_bytes >= populated.rhs_live_entry_bytes);
  CHECK(populated.retained_history_entries == 1);
  CHECK(populated.history_value_bytes == sizeof(HistoryEntry));
  CHECK(populated.active_commit_envelopes == 0);
  CHECK(populated.prepared_current_bytes == 0);
  CHECK(populated.prepared_peak_bytes > 0);
  CHECK(populated.total_accounted_bytes > empty.total_accounted_bytes);

  Transaction erase;
  erase.remove(make_normalized_key("memory", 0), make_normalized_key(9999));
  CHECK(index.apply_commit(erase) == 3);
  const auto after_delete = index.memory_statistics();
  CHECK(after_delete.distinct_rhs_values == key_count);
  CHECK(after_delete.row_multiplicity == key_count);
  CHECK(after_delete.rhs_allocated_bytes == populated.rhs_allocated_bytes);
  CHECK(after_delete.rhs_live_entry_bytes < populated.rhs_live_entry_bytes);
}

static void test_od_with_composite_normalized_keys_and_tombstone() {
  DVTree index(DependencyKind::OD);
  const auto a = make_normalized_key("region", 1, "a");
  const auto b = make_normalized_key("region", 1, "b");
  const auto c = make_normalized_key("region", 2, "a");
  const auto ten = make_normalized_key(10);
  const auto twenty = make_normalized_key(20);
  const auto thirty = make_normalized_key(30);

  Transaction seed;
  seed.insert(a, ten);
  seed.insert(b, thirty);
  seed.insert(c, twenty);
  CHECK(index.apply_commit(seed) == 1);
  CHECK(!index.holds());
  CHECK(index.violations() == 1);
  CHECK(index.find(b)->neighbor_violation == 1);

  // Tombstone b. OD comparison must skip it and compare a directly with c.
  Transaction tombstone;
  tombstone.remove(b, thirty);
  CHECK(index.apply_commit(tombstone) == 2);
  CHECK(index.holds());
  CHECK(index.find(b)->rhs_counts.empty());
  CHECK(index.first_entry()->right == index.find(b));  // stable structural entry remains
}

static void test_transactions_snapshots_and_watermarks() {
  DVTree index(DependencyKind::FD, 8);
  const auto lhs = make_normalized_key("account", 7);
  const auto ten = make_normalized_key(10);
  const auto twenty = make_normalized_key(20);

  Transaction seed;
  seed.insert(lhs, ten);
  CHECK(index.apply_commit(seed) == 1);
  CHECK(index.holds());
  CHECK(index.violations_exact(CommitID{1}) == 0);
  CHECK(index.history().size() == 0);  // zero-delta commits consume no retained entry
  CHECK(index.find(lhs)->version == 1);

  Transaction violate;
  violate.insert(lhs, twenty);
  CHECK(index.apply_commit(violate) == 2);
  CHECK(index.violations() == 1);
  CHECK(index.holds_exact(CommitID{1}));
  CHECK(!index.holds_exact(CommitID{2}));
  CHECK(index.find(lhs)->version == 2);

  Transaction repair;
  repair.remove(lhs, twenty);
  CHECK(index.apply_commit(repair) == 3);
  CHECK(index.violations() == 0);
  CHECK(index.violations_exact(CommitID{1}) == 0);
  CHECK(index.violations_exact(CommitID{2}) == 1);
  CHECK(index.violations_exact(CommitID{3}) == 0);
  CHECK(index.find(lhs)->version == 3);

  Transaction update;
  update.update(lhs, ten, twenty);
  CHECK(index.apply_commit(update) == 4);
  CHECK(index.holds());  // one RHS replaced by one RHS
  CHECK(index.find(lhs)->rhs_counts.find(std::string_view(twenty)) != index.find(lhs)->rhs_counts.end());
}

static void test_transaction_is_private_until_applied() {
  DVTree index(DependencyKind::FD);
  const auto lhs = make_normalized_key(1);
  Transaction transaction;
  transaction.insert(lhs, make_normalized_key(10));
  transaction.insert(lhs, make_normalized_key(20));
  CHECK(index.violations() == 0);
  CHECK(index.holds());
  CHECK(index.apply_commit(transaction) == 1);
  CHECK(index.violations() == 1);
  CHECK(!index.holds());
}

static void test_history_window_exact_reads_and_horizon() {
  VersionedViolationHistory history(2);
  history.update(1, 1);
  history.update(2, 2);
  CHECK(history.query(1) == 1);
  CHECK(history.query(2) == 3);
  history.update(3, -1);  // cid 1 folds into the baseline
  CHECK(history.query(2) == 3);
  CHECK(history.query(3) == 2);
  CHECK(history.can_query_exactly(1));
  CHECK(!history.can_query_exactly(0));

  bool exact_threw = false;
  try {
    (void)history.query_exact(0);
  } catch (const SnapshotHorizonViolation&) {
    exact_threw = true;
  }
  CHECK(exact_threw);

  VersionedViolationHistory protected_history(1);
  protected_history.update(1, 1);
  protected_history.set_lowest_active(1);
  protected_history.update(2, 1);  // protected active window grows beyond the soft cap
  CHECK(protected_history.capacity() == 1);
  CHECK(protected_history.size() == 2);
  CHECK(protected_history.query_exact(1) == 1);
  CHECK(protected_history.query_exact(2) == 2);
  protected_history.set_lowest_active(2);
  CHECK(protected_history.size() == 1);  // cid 1 folded once no active reader needs it
  CHECK(protected_history.query_exact(1) == 1);
  CHECK(protected_history.query_latest() == 2);

  protected_history.update(3, 1);  // cid 2 is protected, so growth repeats safely
  CHECK(protected_history.size() == 2);
  protected_history.clear_lowest_active();
  CHECK(protected_history.size() == 1);
  CHECK(protected_history.query_latest() == 3);
}

static void test_reserved_history_append_lifecycle() {
  VersionedViolationHistory history(1);

  // Reservation is the potentially allocating preparation step. Consumption
  // is what the irreversible commit path uses afterwards.
  history.reserve_append();
  history.update_reserved(1, 1);
  CHECK(history.query_exact(1) == 1);

  // A zero delta still consumes its reservation: a no-op/aborted ticket must
  // never leave capacity bookkeeping that could affect a later commit.
  history.reserve_append();
  history.update_reserved(2, 0);
  CHECK(history.query_latest() == 1);

  history.reserve_append();
  history.cancel_reserved_append();
  bool missing_reservation_threw = false;
  try {
    history.update_reserved(3, 1);
  } catch (const HistoryOrderViolation&) {
    missing_reservation_threw = true;
  }
  CHECK(missing_reservation_threw);
}

static void test_history_coalescing_and_repeated_folding() {
  VersionedViolationHistory history(3);
  history.update(10, 1);
  history.update(10, 2);
  history.update(10, -1);
  CHECK(history.size() == 1);
  CHECK(history.query(10) == 2);

  history.update(20, -1);
  history.update(30, 4);
  history.update(40, -2);
  history.update(50, 3);
  history.update(60, -1);
  CHECK(history.size() == 3);
  CHECK(history.evicted());
  CHECK(history.query_latest() == 5);
  CHECK(history.query(40) == 3);
  CHECK(history.query(50) == 6);
  CHECK(history.query(60) == 5);
  CHECK(history.can_query_exactly(30));
  CHECK(!history.can_query_exactly(29));
}

static void test_fd_duplicate_delete_tombstone_and_resurrection_lifecycle() {
  DVTree index(DependencyKind::FD);
  const std::string lhs = make_normalized_key("lifecycle", 5);
  const std::string ten = make_normalized_key(10);
  const std::string twenty = make_normalized_key(20);

  Transaction seed;
  seed.insert(lhs, ten);
  seed.insert(lhs, ten);
  CHECK(index.apply_commit(seed) == 1);
  DependencyEntry* entry = index.find(lhs);
  CHECK(entry != nullptr);
  CHECK(entry->distinct_rhs() == 1);
  CHECK(entry->rhs_counts.begin()->second == 2);
  CHECK(index.holds());

  Transaction first_delete;
  first_delete.remove(lhs, ten);
  CHECK(index.apply_commit(first_delete) == 2);
  CHECK(entry->distinct_rhs() == 1);
  CHECK(entry->rhs_counts.begin()->second == 1);

  Transaction tombstone;
  tombstone.remove(lhs, ten);
  CHECK(index.apply_commit(tombstone) == 3);
  CHECK(entry->rhs_counts.empty());
  CHECK(index.holds());

  Transaction resurrect;
  resurrect.insert(lhs, ten);
  resurrect.insert(lhs, twenty);
  CHECK(index.apply_commit(resurrect) == 4);
  CHECK(index.find(lhs) == entry);  // resurrected the structural entry; no duplicate key
  CHECK(entry->distinct_rhs() == 2);
  CHECK(entry->local_violations == 1);
  CHECK(index.violations_exact(CommitID{3}) == 0);
  CHECK(index.violations_exact(CommitID{4}) == 1);
}

static void test_od_across_long_tombstone_run_and_resurrection() {
  DVTree index(DependencyKind::OD);
  const std::string a = make_normalized_key(1);
  const std::string b = make_normalized_key(2);
  const std::string c = make_normalized_key(3);
  const std::string d = make_normalized_key(4);
  const std::string r100 = make_normalized_key(100);
  const std::string r50 = make_normalized_key(50);
  const std::string r25 = make_normalized_key(25);
  const std::string r0 = make_normalized_key(0);

  Transaction seed;
  seed.insert(a, r100);
  seed.insert(b, r50);
  seed.insert(c, r25);
  seed.insert(d, r0);
  CHECK(index.apply_commit(seed) == 1);
  CHECK(index.violations() == 3);

  Transaction remove_middle;
  remove_middle.remove(b, r50);
  remove_middle.remove(c, r25);
  CHECK(index.apply_commit(remove_middle) == 2);
  CHECK(index.violations() == 1);  // A compares through B,C directly with D
  CHECK(index.find(b)->neighbor_violation == 0);
  CHECK(index.find(c)->neighbor_violation == 0);

  Transaction resurrect_b;
  resurrect_b.insert(b, r50);
  CHECK(index.apply_commit(resurrect_b) == 3);
  CHECK(index.violations() == 2);  // A|B and B|D

  Transaction resurrect_c;
  resurrect_c.insert(c, r25);
  CHECK(index.apply_commit(resurrect_c) == 4);
  CHECK(index.violations() == 3);
  CHECK(index.find(a)->right == index.find(b));
  CHECK(index.find(b)->right == index.find(c));
}

static void test_dropped_transaction_and_update_semantics() {
  DVTree index(DependencyKind::FD);
  const std::string lhs = make_normalized_key(9);
  const std::string ten = make_normalized_key(10);
  const std::string twenty = make_normalized_key(20);

  Transaction seed;
  seed.insert(lhs, ten);
  CHECK(index.apply_commit(seed) == 1);
  const std::size_t history_before = index.history().size();
  {
    Transaction dropped;
    dropped.insert(lhs, twenty);
    // Deliberately never handed to apply_commit().
  }
  CHECK(index.holds());
  CHECK(index.find(lhs)->distinct_rhs() == 1);
  CHECK(index.history().size() == history_before);

  Transaction update;
  update.update(lhs, ten, twenty);
  CHECK(index.apply_commit(update) == 2);
  CHECK(index.holds());
  CHECK(index.find(lhs)->distinct_rhs() == 1);
  CHECK(index.find(lhs)->rhs_counts.find(std::string_view(twenty)) != index.find(lhs)->rhs_counts.end());
}

static void test_transaction_failure_leaves_committed_metadata_unchanged() {
  DVTree index(DependencyKind::FD, 8);
  const std::string lhs = make_normalized_key("atomic-failure");
  const std::string ten = make_normalized_key(10);
  const std::string twenty = make_normalized_key(20);
  const std::string missing = make_normalized_key(999);
  Transaction seed;
  seed.insert(lhs, ten);
  CHECK(index.apply_commit(seed) == 1);
  const auto before = index.snapshot_entry(lhs);

  Transaction invalid;
  invalid.insert(lhs, twenty);
  invalid.remove(lhs, missing);
  bool missing_threw = false;
  try {
    CHECK(index.apply_commit(invalid) == 2);
  } catch (const MissingDependencyRow&) {
    missing_threw = true;
  }
  CHECK(missing_threw);
  CHECK(index.visible_commit_id() == 2);  // failed convenience commit retires as a no-op
  CHECK(index.violations() == 0);
  const auto after = index.snapshot_entry(lhs);
  CHECK(before.has_value() && after.has_value());
  CHECK(after->rhs_counts == before->rhs_counts);
  CHECK(after->local_violations == before->local_violations);
  CHECK(after->version == before->version);

  Transaction violate;
  violate.insert(lhs, twenty);
  CHECK(index.apply_commit(violate) == 3);
  index.set_lowest_active_snapshot(CommitID{3});
  Transaction horizon_rejected;
  horizon_rejected.insert(lhs, make_normalized_key(30));
  CHECK(index.apply_commit(horizon_rejected) == 4);
  CHECK(index.visible_commit_id() == 4);
  CHECK(index.snapshot_entry(lhs)->rhs_counts.size() == 3);

  DVTree protected_index(DependencyKind::FD, 1);
  CHECK(protected_index.apply_commit(seed) == 1);
  CHECK(protected_index.apply_commit(violate) == 2);  // first non-zero history entry
  protected_index.set_lowest_active_snapshot(CommitID{2});
  CHECK(protected_index.apply_commit(horizon_rejected) == 3);
  CHECK(protected_index.visible_commit_id() == 3);
  CHECK(protected_index.violations() == 2);
  CHECK(protected_index.violations_exact(CommitID{2}) == 1);
  CHECK(protected_index.violations_exact(CommitID{3}) == 2);
  CHECK(protected_index.snapshot_entry(lhs)->rhs_counts.size() == 3);
  CHECK(protected_index.history().capacity() == 1);
  CHECK(protected_index.history().size() == 2);
  protected_index.set_lowest_active_snapshot(CommitID{3});
  CHECK(protected_index.history().size() == 1);
  CHECK(protected_index.violations_exact(CommitID{2}) == 1);
}

static void test_od_failed_transaction_preserves_neighbors_and_history() {
  DVTree index(DependencyKind::OD, 16);
  const std::string a = make_normalized_key(1);
  const std::string b = make_normalized_key(2);
  const std::string c = make_normalized_key(3);
  const std::string ten = make_normalized_key(10);
  const std::string twenty = make_normalized_key(20);
  const std::string thirty = make_normalized_key(30);
  Transaction seed;
  seed.insert(a, ten);
  seed.insert(b, twenty);
  seed.insert(c, thirty);
  CHECK(index.apply_commit(seed) == 1);
  const auto before_a = index.snapshot_entry(a);
  const auto before_b = index.snapshot_entry(b);

  Transaction invalid;
  invalid.remove(b, twenty);
  invalid.remove(c, make_normalized_key(999));
  bool threw = false;
  try {
    CHECK(index.apply_commit(invalid) == 2);
  } catch (const MissingDependencyRow&) {
    threw = true;
  }
  CHECK(threw);
  CHECK(index.visible_commit_id() == 2);  // failed CID retires without effects
  CHECK(index.violations() == 0);
  CHECK(index.snapshot_entry(a)->neighbor_violation == before_a->neighbor_violation);
  CHECK(index.snapshot_entry(b)->rhs_counts == before_b->rhs_counts);
  CHECK(index.snapshot_entry(b)->version == before_b->version);
}

static void test_incremental_matches_batch_for_fd_and_od() {
  for (DependencyKind kind : {DependencyKind::FD, DependencyKind::OD}) {
    DVTree index(kind, 2048);
    std::vector<std::pair<int, int>> rows;
    std::mt19937 generator(kind == DependencyKind::FD ? 0xFD123u : 0x0D123u);

    for (int step = 0; step < 800; ++step) {
      const bool remove = !rows.empty() && (generator() % 3 == 0);
      Transaction transaction;
      if (remove) {
        const std::size_t position = generator() % rows.size();
        const auto row = rows[position];
        transaction.remove(make_normalized_key(row.first), make_normalized_key(row.second));
        rows.erase(rows.begin() + static_cast<std::ptrdiff_t>(position));
      } else {
        const int lhs = static_cast<int>(generator() % 50);
        const int rhs = static_cast<int>(generator() % 80);
        transaction.insert(make_normalized_key(lhs), make_normalized_key(rhs));
        rows.emplace_back(lhs, rhs);
      }
      CHECK(index.apply_commit(transaction) == static_cast<CommitID>(step + 1));

      if (step % 41 == 0 || step == 799) {
        const int64_t incremental = index.violations_live_uncommitted();
        index.compute_verdict();
        CHECK(index.violations_live_uncommitted() == incremental);
        CHECK(index.violations() == incremental);
      }
    }
  }
}

static void test_registered_optimistic_completion_lifecycle() {
  const std::string lhs = make_normalized_key("registered-lifecycle");
  const std::string a = make_normalized_key(10);
  const std::string b = make_normalized_key(20);

  DVTree index(DependencyKind::FD, 16);
  Transaction seed;
  seed.insert(lhs, a);
  CHECK(index.apply_commit(seed) == 1);

  auto first = index.begin_commit();
  auto second = index.begin_commit();
  first.insert(lhs, b);
  second.remove(lhs, b);

  // CID 3 may prepare first, but cannot become visible across the reserved
  // CID-2 gap. Once CID 2 becomes visible, CID 3 is recomputed from that state.
  second.seal();
  CHECK(index.visible_commit_id() == 1);
  first.seal();
  first.wait_until_visible();
  second.wait_until_visible();
  CHECK(index.visible_commit_id() == 3);
  CHECK(index.violations_exact(CommitID{2}) == 1);
  CHECK(index.violations_exact(CommitID{3}) == 0);
  CHECK(index.snapshot_entry(lhs)->rhs_counts.size() == 1);

  // A definitive error at the head stays invisible. Explicit abort turns
  // the already-reserved CID into a no-op and lets later work continue.
  auto invalid = index.begin_commit();
  invalid.remove(lhs, b);
  invalid.seal();
  bool rejected = false;
  try {
    invalid.wait_until_visible();
  } catch (const MissingDependencyRow&) {
    rejected = true;
  }
  CHECK(rejected);
  CHECK(index.visible_commit_id() == 3);
  invalid.abort();
  invalid.wait_until_visible();
  CHECK(index.visible_commit_id() == 4);

  bool stage_after_resolution_rejected = false;
  try {
    invalid.insert(lhs, b);
  } catch (const CommitOrderViolation&) {
    stage_after_resolution_rejected = true;
  }
  CHECK(stage_after_resolution_rejected);
}

int main() {
  CHECK(public_api_smoke_test());
  test_hyrise_commit_id_boundaries();
  test_external_sparse_commit_ids_and_registration_frontier();
  test_external_out_of_order_registration_and_rejections();
  test_normalization_order_and_boundaries();
  test_duckdb_nan_and_null_normalization_policy();
  test_duckdb_nan_and_null_dependency_semantics();
  test_upstream_engine_with_variable_composite_keys();
  test_engine_neighbors_across_pages();
  test_page_layout_and_control_are_separate();
  test_olc_version_wrap_preserves_lock_state();
  test_stale_split_prefix_forces_restart();
  test_failed_page_publication_preserves_reader_image();
  test_page_image_allocation_fails_before_write_lock();
  test_oversized_key_fails_before_upstream_assert();
  test_fd_metadata_is_out_of_line();
  test_memory_statistics_accounting();
  test_od_with_composite_normalized_keys_and_tombstone();
  test_transactions_snapshots_and_watermarks();
  test_transaction_is_private_until_applied();
  test_history_window_exact_reads_and_horizon();
  test_reserved_history_append_lifecycle();
  test_history_coalescing_and_repeated_folding();
  test_fd_duplicate_delete_tombstone_and_resurrection_lifecycle();
  test_od_across_long_tombstone_run_and_resurrection();
  test_dropped_transaction_and_update_semantics();
  test_transaction_failure_leaves_committed_metadata_unchanged();
  test_od_failed_transaction_preserves_neighbors_and_history();
  test_incremental_matches_batch_for_fd_and_od();
  test_registered_optimistic_completion_lifecycle();

  if (failures != 0) {
    std::cerr << failures << " of " << checks << " checks failed\n";
    return 1;
  }
  std::cout << "all " << checks << " checks passed\n";
  return 0;
}
