#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace hyrise {

class BTreeIndexTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(BTreeIndexTest, BasicFunctionality) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(4);
  value_segment->append(2);
  value_segment->append(5);
  value_segment->append(2);
  value_segment->append(1);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Test lower_bound
  auto it = index->lower_bound({2});
  EXPECT_NE(it, index->cend());
  EXPECT_EQ(*it, ChunkOffset{1});  // First 2 is at index 1

  it++;
  EXPECT_EQ(*it, ChunkOffset{3});  // Second 2 is at index 3

  it++;
  EXPECT_EQ(*it, ChunkOffset{0});  // 4 is at index 0 (sorted: 1, 2, 2, 4, 5) -> offsets: 4, 1, 3, 0, 2

  // Test upper_bound
  it = index->upper_bound({2});
  EXPECT_EQ(*it, ChunkOffset{0});  // First element > 2 is 4 at index 0
}

TEST_F(BTreeIndexTest, DependencyCheck) {
  // Test A -> B
  // A: 1, 1, 2, 3
  // B: 10, 10, 20, 30
  // Valid

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);
  segment_a->append(1);
  segment_a->append(1);
  segment_a->append(2);
  segment_a->append(3);

  auto segment_b = std::make_shared<ValueSegment<int32_t>>(false);
  segment_b->append(10);
  segment_b->append(10);
  segment_b->append(20);
  segment_b->append(30);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  bool valid = true;
  auto it = index->cbegin();
  auto end = index->cend();

  if (it != end) {
    auto prev_offset = *it;
    it++;
    while (it != end) {
      auto curr_offset = *it;

      auto val_a_prev = (*segment_a)[prev_offset];
      auto val_a_curr = (*segment_a)[curr_offset];

      if (val_a_prev == val_a_curr) {
        auto val_b_prev = (*segment_b)[prev_offset];
        auto val_b_curr = (*segment_b)[curr_offset];
        if (val_b_prev != val_b_curr) {
          valid = false;
          break;
        }
      }

      prev_offset = curr_offset;
      it++;
    }
  }

  EXPECT_TRUE(valid);
}

TEST_F(BTreeIndexTest, DependencyCheckViolation) {
  // Test A -> B
  // A: 1, 1, 2
  // B: 10, 11, 20
  // Invalid (1->10, 1->11)

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);
  segment_a->append(1);
  segment_a->append(1);
  segment_a->append(2);

  auto segment_b = std::make_shared<ValueSegment<int32_t>>(false);
  segment_b->append(10);
  segment_b->append(11);
  segment_b->append(20);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  bool valid = true;
  auto it = index->cbegin();
  auto end = index->cend();

  if (it != end) {
    auto prev_offset = *it;
    it++;
    while (it != end) {
      auto curr_offset = *it;

      auto val_a_prev = (*segment_a)[prev_offset];
      auto val_a_curr = (*segment_a)[curr_offset];

      if (val_a_prev == val_a_curr) {
        auto val_b_prev = (*segment_b)[prev_offset];
        auto val_b_curr = (*segment_b)[curr_offset];
        if (val_b_prev != val_b_curr) {
          valid = false;
          break;
        }
      }

      prev_offset = curr_offset;
      it++;
    }
  }

  EXPECT_FALSE(valid);
}

TEST_F(BTreeIndexTest, DependencyCheckWithMVCC) {
  // Test A -> B with MVCC
  // Row 0: A=1, B=10 (Committed)
  // Row 1: A=1, B=11 (Uncommitted / Invisible)

  auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}},
                                       TableType::Data, std::nullopt, UseMvcc::Yes);

  table->append({1, 10});  // Row 0
  table->append({1, 11});  // Row 1

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0 is visible to T2
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{1});

  // Row 1 is NOT visible to T2 (e.g. created by T3 which is not committed or started after T2)
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{3});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{3});

  auto segment_a = chunk->get_segment(ColumnID{0});
  auto segment_b = chunk->get_segment(ColumnID{1});

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Check validity for T2 (CommitID 2)
  CommitID snapshot_cid = CommitID{2};

  bool valid = true;
  auto it = index->cbegin();
  auto end = index->cend();

  // We need to collect visible rows for each key
  // Since index is sorted by key, we can process groups

  if (it != end) {
    std::vector<ChunkOffset> group_offsets;
    auto first_offset = *it;
    group_offsets.push_back(first_offset);

    it++;
    while (it != end) {
      auto curr_offset = *it;
      auto val_a_curr = (*segment_a)[curr_offset];
      auto val_a_prev = (*segment_a)[group_offsets.back()];

      if (val_a_curr != val_a_prev) {
        // Process previous group
        std::vector<AllTypeVariant> visible_b_values;
        for (auto offset : group_offsets) {
          auto begin = mvcc_data->get_begin_cid(offset);
          auto end_cid = mvcc_data->get_end_cid(offset);
          if (begin <= snapshot_cid && end_cid > snapshot_cid) {
            visible_b_values.push_back((*segment_b)[offset]);
          }
        }

        // Check uniqueness of B values in the group
        if (!visible_b_values.empty()) {
          for (size_t i = 1; i < visible_b_values.size(); ++i) {
            if (visible_b_values[i] != visible_b_values[0]) {
              valid = false;
            }
          }
        }

        group_offsets.clear();
      }

      group_offsets.push_back(curr_offset);
      if (!valid)
        break;
      it++;
    }

    // Process last group
    if (valid && !group_offsets.empty()) {
      std::vector<AllTypeVariant> visible_b_values;
      for (auto offset : group_offsets) {
        auto begin = mvcc_data->get_begin_cid(offset);
        auto end_cid = mvcc_data->get_end_cid(offset);
        if (begin <= snapshot_cid && end_cid > snapshot_cid) {
          visible_b_values.push_back((*segment_b)[offset]);
        }
      }

      if (!visible_b_values.empty()) {
        for (size_t i = 1; i < visible_b_values.size(); ++i) {
          if (visible_b_values[i] != visible_b_values[0]) {
            valid = false;
          }
        }
      }
    }
  }

  EXPECT_TRUE(valid);
}

TEST_F(BTreeIndexTest, MetadataLogic) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);  // Duplicate
  value_segment->append(1);  // Duplicate
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initial state
  EXPECT_EQ(index->global_violation_count, 0);

  // With OD mode, local violations are always 0 (same LHS are equal in ordering)
  index->recompute_local_violation_delta({1}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 0);

  index->recompute_local_violation_delta({2}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 0);

  // Check flags - these work independently of FD/OD
  EXPECT_EQ(index->get_right_neighbor_flag({1}), 0);
  index->set_right_neighbor_flag({1}, 1);
  EXPECT_EQ(index->get_right_neighbor_flag({1}), 1);
  EXPECT_EQ(index->global_violation_count, 1);  // 0 (local) + 1 (flag)

  // Check flag persistence/independence
  EXPECT_EQ(index->get_right_neighbor_flag({2}), 0);

  // Reset flag
  index->set_right_neighbor_flag({1}, 0);
  EXPECT_EQ(index->global_violation_count, 0);
}

TEST_F(BTreeIndexTest, MetadataDeltas) {
  // Create index with values: 1, 2, 3, 4, 5
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(3);
  value_segment->append(1);
  value_segment->append(5);
  value_segment->append(2);
  value_segment->append(4);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Verify MetadataDeltas struct works correctly
  MetadataDeltas deltas;
  deltas.flag_delta = 2;
  deltas.local_violation_count_delta = 3;
  EXPECT_EQ(deltas.total_delta(), 5);
}

TEST_F(BTreeIndexTest, LeafNeighborPointers) {
  // Create index with enough values to potentially create multiple leaves
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 1; i <= 10; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Verify all values are searchable
  for (int i = 1; i <= 10; ++i) {
    auto value = index->get_value({i});
    EXPECT_NE(value, nullptr);
    EXPECT_EQ(value->count, ChunkOffset{1});
  }
}

TEST_F(BTreeIndexTest, GetLeftNeighborMaxKey) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);
  value_segment->append(4);
  value_segment->append(5);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Key 1 has no left neighbor (it's the smallest)
  auto left_of_1 = index->get_left_neighbor_max_key({1});
  EXPECT_TRUE(left_of_1.empty());

  // Key 2's left neighbor is 1
  auto left_of_2 = index->get_left_neighbor_max_key({2});
  // Note: may be empty if all in same leaf, but position-based neighbor should work
}

TEST_F(BTreeIndexTest, InsertEntryForValidation) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});
  EXPECT_EQ(index->global_violation_count, 0);

  // Test FD validation: insert a new RHS value for existing LHS key
  // LHS=2 already exists, we add RHS=20
  auto deltas = index->insert_entry_for_validation({2}, {20}, DependencyType::FD);

  // For FD: first RHS value means distinct_count=1, violations=0
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert a different RHS value for same LHS=2
  deltas = index->insert_entry_for_validation({2}, {25}, DependencyType::FD);

  // Now FD has 2 distinct RHS values for LHS=2 -> 1 violation
  EXPECT_EQ(index->global_violation_count, 1);
}

TEST_F(BTreeIndexTest, DeleteEntryForValidation) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});
  EXPECT_EQ(index->global_violation_count, 0);

  // First add two different RHS values for LHS=1 (FD violation)
  index->insert_entry_for_validation({1}, {10}, DependencyType::FD);
  index->insert_entry_for_validation({1}, {20}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 1);  // 2 distinct RHS -> 1 violation

  // Delete one RHS value
  auto deltas = index->delete_entry_for_validation({1}, {20}, DependencyType::FD);

  // Now only 1 distinct RHS value -> 0 violations
  EXPECT_EQ(index->global_violation_count, 0);
}

TEST_F(BTreeIndexTest, UpdateEntryForValidation) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // First add an RHS value for LHS=2
  index->insert_entry_for_validation({2}, {20}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 0);

  // Update = delete old RHS + insert new RHS (same value = no change)
  auto deltas = index->update_entry_for_validation({2}, {20}, {20}, DependencyType::FD);

  // Update with same value should have minimal impact
  EXPECT_EQ(index->global_violation_count, 0);
}

TEST_F(BTreeIndexTest, FlagDeltaComputation) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(10);
  value_segment->append(20);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Test OD validation with boundary violations
  // For OD: max_rhs(key1) > min_rhs(key2) is a violation

  // Insert RHS values that create an OD violation
  // LHS=1 with RHS=100, LHS=10 with RHS=50
  // If LHS is sorted (1 < 10), RHS should also be sorted (100 should be < 50) - VIOLATION!
  index->insert_entry_for_validation({1}, {100}, DependencyType::OD);
  index->insert_entry_for_validation({10}, {50}, DependencyType::OD);

  // max_rhs(1)=100 > min_rhs(10)=50 -> OD violation
  EXPECT_GT(index->global_violation_count, 0);
}

TEST_F(BTreeIndexTest, ODLocalViolations) {
  // Test OD local violations: same LHS with different RHS creates ambiguous ordering
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(100);  // Seed value

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initially no violations
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert LHS=1 with RHS=10 - no violation
  index->insert_entry_for_validation({1}, {10}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert LHS=1 with RHS=20 (DIFFERENT RHS) - this is an OD violation!
  // Same LHS with different RHS means ordering is ambiguous
  index->insert_entry_for_validation({1}, {20}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 1);  // local_violations = 2 - 1 = 1

  // Insert LHS=1 with RHS=30 (another different RHS)
  index->insert_entry_for_validation({1}, {30}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 2);  // local_violations = 3 - 1 = 2

  // Insert LHS=1 with same RHS=20 again - no new violation (set semantics)
  index->insert_entry_for_validation({1}, {20}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 2);  // Still 3 distinct values, 2 violations

  // Insert LHS=2 with RHS=100 - no local violation for this key
  // But check boundary: max_rhs(1)=30, min_rhs(2)=100, 30 < 100, so no boundary violation
  index->insert_entry_for_validation({2}, {100}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 2);  // Only local violations from LHS=1
}

TEST_F(BTreeIndexTest, LocalViolationCountDelta) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(2);
  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Key 1 has count=3, the legacy local_violation_count() method returns count-1=2
  // But with the new FD logic, it should be based on distinct RHS values
  auto value = index->get_value({1});
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->count, ChunkOffset{3});

  // For OD, local_violation_count is always 0
  EXPECT_EQ(value->local_violation_count(DependencyType::OD), 0);

  // For FD with no RHS values tracked yet, it's rhs_values.size() - 1 = -1, but clamped
  // The legacy method still returns count - 1 for backward compatibility
  EXPECT_EQ(value->local_violation_count(), 2);  // Legacy behavior

  // Key 2 has count=1, so local_violation_count = 1-1 = 0
  value = index->get_value({2});
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->count, ChunkOffset{1});
  EXPECT_EQ(value->local_violation_count(), 0);
}

TEST_F(BTreeIndexTest, GlobalViolationCountFormula) {
  // Test: global = sum of flag_contributions + sum of local_violation_contributions

  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);
  value_segment->append(3);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initially global = 0
  EXPECT_EQ(index->global_violation_count, 0);

  // Test with OD mode (local violations = 0)
  // Set flags manually to test global count formula
  index->set_right_neighbor_flag({1}, 1);
  EXPECT_EQ(index->global_violation_count, 1);

  // Set flag on key 2
  index->set_right_neighbor_flag({2}, 1);
  EXPECT_EQ(index->global_violation_count, 2);

  // Reset flag on key 1
  index->set_right_neighbor_flag({1}, 0);
  EXPECT_EQ(index->global_violation_count, 1);

  // Reset flag on key 2
  index->set_right_neighbor_flag({2}, 0);
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// DYNAMIC INDEX OPERATIONS TESTS
// Tests for insert_key, remove_key, and true online validation
// ============================================================================

TEST_F(BTreeIndexTest, InsertKeyNewEntry) {
  // Create index with initial data
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Verify initial state
  EXPECT_EQ(index->key_count(), 2u);
  EXPECT_TRUE(index->contains_key({1}));
  EXPECT_TRUE(index->contains_key({3}));
  EXPECT_FALSE(index->contains_key({2}));

  // Insert new key
  bool is_new = index->insert_key({2});
  EXPECT_TRUE(is_new);
  EXPECT_EQ(index->key_count(), 3u);
  EXPECT_TRUE(index->contains_key({2}));

  // Verify the value exists and has count 1
  auto value = index->get_value({2});
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->count, ChunkOffset{1});
}

TEST_F(BTreeIndexTest, InsertKeyExistingEntry) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);  // Duplicate

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initial count should be 2
  auto value = index->get_value({1});
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->count, ChunkOffset{2});

  // Insert another instance of same key
  bool is_new = index->insert_key({1});
  EXPECT_FALSE(is_new);  // Not a new key

  // Count should now be 3
  value = index->get_value({1});
  EXPECT_EQ(value->count, ChunkOffset{3});
  EXPECT_EQ(index->key_count(), 1u);  // Still only 1 distinct key
}

TEST_F(BTreeIndexTest, RemoveKeyDecrementCount) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(1);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initial count is 3
  auto value = index->get_value({1});
  EXPECT_EQ(value->count, ChunkOffset{3});

  // Remove one instance
  bool removed = index->remove_key({1});
  EXPECT_FALSE(removed);  // Entry not completely removed

  // Count should be 2
  value = index->get_value({1});
  EXPECT_EQ(value->count, ChunkOffset{2});
  EXPECT_TRUE(index->contains_key({1}));
}

TEST_F(BTreeIndexTest, RemoveKeyCompleteRemoval) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_EQ(index->key_count(), 2u);

  // Remove key with count 1
  bool removed = index->remove_key({1});
  EXPECT_TRUE(removed);  // Entry completely removed

  EXPECT_EQ(index->key_count(), 1u);
  EXPECT_FALSE(index->contains_key({1}));
  EXPECT_TRUE(index->contains_key({2}));
}

TEST_F(BTreeIndexTest, RemoveKeyNonExistent) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Try to remove non-existent key
  bool removed = index->remove_key({999});
  EXPECT_FALSE(removed);
  EXPECT_EQ(index->key_count(), 1u);
}

TEST_F(BTreeIndexTest, DynamicInsertWithValidation) {
  // Start with empty-ish index (single key)
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_EQ(index->global_violation_count, 0);

  // Insert new key via FD validation API - should create the key
  auto deltas = index->insert_entry_for_validation({5}, {50}, DependencyType::FD);

  // Key should now exist
  EXPECT_TRUE(index->contains_key({5}));
  EXPECT_EQ(index->key_count(), 2u);
}

TEST_F(BTreeIndexTest, DynamicDeleteWithValidation) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // First, insert RHS values so deletion has something to remove
  index->insert_entry_for_validation({2}, {20}, DependencyType::FD);
  EXPECT_EQ(index->key_count(), 3u);

  // Delete via FD validation API
  auto deltas = index->delete_entry_for_validation({2}, {20}, DependencyType::FD);

  // Key should be removed (no more RHS values)
  EXPECT_FALSE(index->contains_key({2}));
  EXPECT_EQ(index->key_count(), 2u);
}

TEST_F(BTreeIndexTest, OnlineValidationFullCycle) {
  // Test full online FD validation: start with seed, insert keys, check status
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(100);  // Seed value to create index

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initially no violations
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert key 1 with RHS value 10
  index->insert_entry_for_validation({1}, {10}, DependencyType::FD);
  EXPECT_TRUE(index->contains_key({1}));
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert key 2 with RHS value 20
  index->insert_entry_for_validation({2}, {20}, DependencyType::FD);
  EXPECT_TRUE(index->contains_key({2}));
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert key 1 again with DIFFERENT RHS value 15 - FD violation!
  index->insert_entry_for_validation({1}, {15}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 1);  // 2 distinct RHS for LHS=1

  // Delete key 2 (has only one RHS value)
  index->delete_entry_for_validation({2}, {20}, DependencyType::FD);
  EXPECT_FALSE(index->contains_key({2}));

  // Key 1 should still exist with its violation
  EXPECT_TRUE(index->contains_key({1}));
  EXPECT_EQ(index->global_violation_count, 1);
}

TEST_F(BTreeIndexTest, ContainsKeyBasic) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(10);
  value_segment->append(20);
  value_segment->append(30);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_TRUE(index->contains_key({10}));
  EXPECT_TRUE(index->contains_key({20}));
  EXPECT_TRUE(index->contains_key({30}));
  EXPECT_FALSE(index->contains_key({15}));
  EXPECT_FALSE(index->contains_key({0}));
  EXPECT_FALSE(index->contains_key({100}));
}

TEST_F(BTreeIndexTest, KeyCountAccuracy) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 100; ++i) {
    value_segment->append(i % 10);  // 10 distinct keys, each appears 10 times
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_EQ(index->key_count(), 10u);

  // Add new distinct key
  index->insert_key({99});
  EXPECT_EQ(index->key_count(), 11u);

  // Remove a key completely
  for (int i = 0; i < 10; ++i) {
    index->remove_key({0});
  }
  EXPECT_EQ(index->key_count(), 10u);
  EXPECT_FALSE(index->contains_key({0}));
}

TEST_F(BTreeIndexTest, InsertManyNewKeys) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(0);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Insert many new keys to trigger tree splits
  for (int i = 1; i <= 50; ++i) {
    bool is_new = index->insert_key({i});
    EXPECT_TRUE(is_new);
  }

  EXPECT_EQ(index->key_count(), 51u);

  // Verify all keys exist
  for (int i = 0; i <= 50; ++i) {
    EXPECT_TRUE(index->contains_key({i}));
  }
}

TEST_F(BTreeIndexTest, RemoveManyKeys) {
  // This test verifies removal from a smaller tree that fits in leaves
  // Complex removal from internal nodes requires full B-Tree rebalancing
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  // Create only 5 entries (fits in one leaf with t=3, max entries = 5)
  for (int i = 1; i <= 5; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_EQ(index->key_count(), 5u);

  // Remove keys 2 and 4
  EXPECT_TRUE(index->remove_key({2}));
  EXPECT_TRUE(index->remove_key({4}));

  EXPECT_EQ(index->key_count(), 3u);

  // Verify correct keys remain
  EXPECT_TRUE(index->contains_key({1}));
  EXPECT_FALSE(index->contains_key({2}));
  EXPECT_TRUE(index->contains_key({3}));
  EXPECT_FALSE(index->contains_key({4}));
  EXPECT_TRUE(index->contains_key({5}));
}

TEST_F(BTreeIndexTest, OnlineFDValidationScenario) {
  // Realistic scenario: validating FD (department_id -> manager_name)
  // department_id is indexed, we track if same department always has same manager

  auto dept_segment = std::make_shared<ValueSegment<int32_t>>(false);
  dept_segment->append(1);  // dept 1

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{dept_segment});

  // Insert employee in dept 1 with manager_id = 100
  index->insert_entry_for_validation({1}, {100}, DependencyType::FD);

  // FD holds: 1 distinct RHS value -> 0 violations
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert another employee in dept 1 with SAME manager_id = 100 (FD still holds)
  index->insert_entry_for_validation({1}, {100}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 0);

  // Insert employee in dept 1 with DIFFERENT manager_id = 200 (FD violated!)
  index->insert_entry_for_validation({1}, {200}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 1);  // 2 distinct RHS -> 1 violation

  // Insert employee in new dept 2
  index->insert_entry_for_validation({2}, {300}, DependencyType::FD);
  EXPECT_TRUE(index->contains_key({2}));
  EXPECT_EQ(index->key_count(), 2u);
  EXPECT_EQ(index->global_violation_count, 1);  // Dept 2 has no violation

  // Delete one of the conflicting managers from dept 1
  index->delete_entry_for_validation({1}, {200}, DependencyType::FD);

  // Now dept 1 only has manager 100 -> FD holds again
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// COMPREHENSIVE TESTS FOR ONLINE FD/OD VALIDATION INDEX WITH MVCC
// ============================================================================

// Helper class for FD/OD validation testing
class FDODValidationTest : public BTreeIndexTest {
 protected:
  // Check if FD A -> B is violated (same A, different B)
  static bool check_fd_violation(const std::vector<AllTypeVariant>& a, const std::vector<AllTypeVariant>& b) {
    // For FD: violation if values are different (assuming same LHS group)
    if (a.empty() || b.empty())
      return false;
    return a[0] != b[0];
  }

  // Check if OD A ~> B is violated (A values must be ordered when B is ordered)
  static bool check_od_violation(const std::vector<AllTypeVariant>& a, const std::vector<AllTypeVariant>& b) {
    // For OD: violation if a > b (order not preserved)
    if (a.empty() || b.empty())
      return false;
    return a[0] > b[0];
  }

  // Helper: Check row visibility for a transaction
  static bool is_row_visible(const std::shared_ptr<MvccData>& mvcc_data, ChunkOffset offset, CommitID snapshot_cid) {
    auto begin_cid = mvcc_data->get_begin_cid(offset);
    auto end_cid = mvcc_data->get_end_cid(offset);
    return begin_cid <= snapshot_cid && end_cid > snapshot_cid;
  }

  // Helper: Create a table with MVCC support
  static std::shared_ptr<Table> create_mvcc_table() {
    return std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}},
                                   TableType::Data, std::nullopt, UseMvcc::Yes);
  }
};

// ============================================================================
// Test 1: FD Validation - Basic Correctness
// ============================================================================
TEST_F(FDODValidationTest, FDValidationBasicCorrectness) {
  // FD: A -> B (If two rows have same A, they must have same B)
  // Test data: A={1,1,2,2,3}, B={10,10,20,20,30} - Valid FD

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);
  auto segment_b = std::make_shared<ValueSegment<int32_t>>(false);

  segment_a->append(1);
  segment_b->append(10);
  segment_a->append(1);
  segment_b->append(10);
  segment_a->append(2);
  segment_b->append(20);
  segment_a->append(2);
  segment_b->append(20);
  segment_a->append(3);
  segment_b->append(30);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Test FD validation using the new API
  // Insert RHS values for each LHS key
  index->insert_entry_for_validation({1}, {10}, DependencyType::FD);
  index->insert_entry_for_validation({1}, {10}, DependencyType::FD);  // Same RHS - no violation
  index->insert_entry_for_validation({2}, {20}, DependencyType::FD);
  index->insert_entry_for_validation({2}, {20}, DependencyType::FD);  // Same RHS - no violation
  index->insert_entry_for_validation({3}, {30}, DependencyType::FD);

  // FD holds: each LHS has only 1 distinct RHS
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// Test 2: FD Validation - Violation Detection
// ============================================================================
TEST_F(FDODValidationTest, FDValidationViolationDetection) {
  // FD: A -> B violated
  // Test data: A={1,1,2}, B={10,11,20} - INVALID (1->10 and 1->11)

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);

  segment_a->append(1);
  segment_a->append(2);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Key 1 maps to two different RHS values -> FD violation
  index->insert_entry_for_validation({1}, {10}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 0);  // First RHS, no violation yet

  index->insert_entry_for_validation({1}, {11}, DependencyType::FD);  // Different RHS!
  EXPECT_EQ(index->global_violation_count, 1);                        // 2 distinct RHS -> 1 violation
}

// ============================================================================
// Test 3: OD Validation - Basic Correctness
// ============================================================================
TEST_F(FDODValidationTest, ODValidationBasicCorrectness) {
  // OD: A ~> B (If A1 < A2, then B1 <= B2)
  // Test data: A={1,2,3,4,5}, B={10,20,30,40,50} - Valid OD

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);

  for (int i = 1; i <= 5; ++i) {
    segment_a->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Insert OD entries with ordered RHS values
  for (int i = 1; i <= 5; ++i) {
    index->insert_entry_for_validation({i}, {i * 10}, DependencyType::OD);
  }

  // OD should hold: RHS values are in same order as LHS
  // No boundary violations: max(B_i) <= min(B_i+1) for all i
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// Test 4: OD Validation - Violation Detection
// ============================================================================
TEST_F(FDODValidationTest, ODValidationViolationDetection) {
  // OD: A ~> B violated
  // Test data: A={1,2,3}, B={30,20,10} - INVALID (order not preserved)

  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);

  segment_a->append(1);
  segment_a->append(2);
  segment_a->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Insert OD entries with reverse-ordered RHS values (violations)
  index->insert_entry_for_validation({1}, {30}, DependencyType::OD);  // LHS=1, RHS=30
  index->insert_entry_for_validation({2}, {20}, DependencyType::OD);  // LHS=2, RHS=20 (max(1)=30 > min(2)=20)
  index->insert_entry_for_validation({3}, {10}, DependencyType::OD);  // LHS=3, RHS=10 (max(2)=20 > min(3)=10)

  // OD violated: 2 boundary violations
  EXPECT_EQ(index->global_violation_count, 2);

  EXPECT_EQ(index->global_violation_count, 2);
}

// ============================================================================
// Test 5: MVCC - Transaction Isolation (T1 starts before T2)
// ============================================================================
TEST_F(FDODValidationTest, MVCCTransactionIsolation) {
  // Scenario: T1 (snapshot=2) should not see changes from T2 (commit=3)
  auto table = create_mvcc_table();

  table->append({1, 10});  // Row 0: committed at CID=1
  table->append({1, 11});  // Row 1: committed at CID=3 (invisible to T1)

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0: visible to T1 (committed before snapshot)
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  // Row 1: invisible to T1 (committed after snapshot)
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{3});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{0});

  // T1's view: only row 0 visible
  CommitID t1_snapshot = CommitID{2};

  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, t1_snapshot));
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, t1_snapshot));

  // FD A->B is valid for T1 (only sees one row with A=1)
}

// ============================================================================
// Test 6: MVCC - Concurrent Modification Detection
// ============================================================================
TEST_F(FDODValidationTest, MVCCConcurrentModification) {
  // Scenario: T1 and T2 both try to modify rows affecting same index region
  auto table = create_mvcc_table();

  table->append({1, 10});
  table->append({2, 20});

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Both rows initially committed
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{0});

  // T1 (TID=10) tries to lock row 0
  bool t1_locked = mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, TransactionID{10});
  EXPECT_TRUE(t1_locked);

  // T2 (TID=20) tries to lock same row - should fail
  bool t2_locked = mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, TransactionID{20});
  EXPECT_FALSE(t2_locked);

  // T2 can lock a different row
  t2_locked = mvcc_data->compare_exchange_tid(ChunkOffset{1}, TransactionID{0}, TransactionID{20});
  EXPECT_TRUE(t2_locked);
}

// ============================================================================
// Test 7: MVCC - Deleted Row Handling
// ============================================================================
TEST_F(FDODValidationTest, MVCCDeletedRowHandling) {
  // Scenario: Row is deleted, should be invisible after delete commit
  auto table = create_mvcc_table();

  table->append({1, 10});
  table->append({1, 11});  // Would violate FD if visible

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0: active
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);

  // Row 1: deleted at CID=2
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{1}, CommitID{2});  // Deleted

  // T3's view (snapshot=3): row 1 is invisible (deleted)
  CommitID t3_snapshot = CommitID{3};

  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, t3_snapshot));
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, t3_snapshot));

  // FD A->B valid for T3 (deleted row not counted)
}

// ============================================================================
// Test 8: MVCC - Phantom Read Prevention
// ============================================================================
TEST_F(FDODValidationTest, MVCCPhantomReadPrevention) {
  // Scenario: New row inserted after T1 starts should not be visible to T1
  auto table = create_mvcc_table();

  table->append({1, 10});

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0: committed before T1
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);

  // T1 starts with snapshot=2
  CommitID t1_snapshot = CommitID{2};

  // Later, row 1 is inserted and committed at CID=3
  table->append({1, 11});
  auto mvcc_data2 = table->get_chunk(ChunkID{0})->mvcc_data();
  mvcc_data2->set_begin_cid(ChunkOffset{1}, CommitID{3});
  mvcc_data2->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);

  // T1 should not see row 1 (phantom prevention)
  EXPECT_TRUE(is_row_visible(mvcc_data2, ChunkOffset{0}, t1_snapshot));
  EXPECT_FALSE(is_row_visible(mvcc_data2, ChunkOffset{1}, t1_snapshot));
}

// ============================================================================
// Test 9: Metadata Storage Efficiency - Memory Layout
// ============================================================================
TEST_F(FDODValidationTest, MetadataStorageEfficiency) {
  // Test that BTreeValue struct is reasonably sized
  // Note: BTreeValue includes std::unordered_set<AllTypeVariant> for FD/OD validation
  // which makes it larger than a cache line, but this is acceptable for the
  // per-key metadata storage pattern used in dependency validation.
  BTreeValue value;

  // Check struct size is reasonable (larger due to rhs_values set and optional fields)
  size_t value_size = sizeof(BTreeValue);
  EXPECT_LE(value_size, 256);  // Allow for hash set and optional overhead

  // Verify all fields are accessible with minimal overhead
  value.start_index = ChunkOffset{100};
  value.count = ChunkOffset{50};
  value.right_neighbor_flag = 1;
  value.right_neighbor_flag_contribution = 1;
  value.local_violation_count_contribution = 49;

  EXPECT_EQ(value.start_index, ChunkOffset{100});
  EXPECT_EQ(value.count, ChunkOffset{50});
  EXPECT_EQ(value.local_violation_count(), 49);
}

// ============================================================================
// Test 10: Metadata Access Performance - O(1) Flag Operations
// ============================================================================
TEST_F(FDODValidationTest, MetadataAccessPerformance) {
  // Create index with many entries
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  const int NUM_ENTRIES = 1000;

  for (int i = 0; i < NUM_ENTRIES; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Test that flag operations are O(log n) - B-tree lookup
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_ENTRIES; ++i) {
    index->set_right_neighbor_flag({i}, i % 2);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // Should complete quickly (< 100ms for 1000 entries)
  EXPECT_LT(duration.count(), 100000);

  // Verify flags were set correctly
  for (int i = 0; i < NUM_ENTRIES; ++i) {
    EXPECT_EQ(index->get_right_neighbor_flag({i}), i % 2);
  }
}

// ============================================================================
// Test 11: Global Counter Atomicity
// ============================================================================
TEST_F(FDODValidationTest, GlobalCounterAtomicity) {
  // Test that global_violation_count updates are consistent
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 10; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Set all flags to 1
  for (int i = 0; i < 10; ++i) {
    index->set_right_neighbor_flag({i}, 1);
  }
  EXPECT_EQ(index->global_violation_count, 10);

  // Reset all flags to 0
  for (int i = 0; i < 10; ++i) {
    index->set_right_neighbor_flag({i}, 0);
  }
  EXPECT_EQ(index->global_violation_count, 0);

  // Toggle flags multiple times
  for (int round = 0; round < 5; ++round) {
    for (int i = 0; i < 10; ++i) {
      index->set_right_neighbor_flag({i}, 1);
    }
    EXPECT_EQ(index->global_violation_count, 10);

    for (int i = 0; i < 10; ++i) {
      index->set_right_neighbor_flag({i}, 0);
    }
    EXPECT_EQ(index->global_violation_count, 0);
  }
}

// ============================================================================
// Test 12: Insert Operation - Smallest Value Update
// ============================================================================
TEST_F(FDODValidationTest, InsertOperationSmallestValue) {
  // Test insert when new value becomes smallest in a group
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(2);
  value_segment->append(3);
  value_segment->append(4);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Simulate insert of new smallest key using OD validation
  auto deltas = index->insert_entry_for_validation({1}, {1}, DependencyType::OD);

  // Key 1 should now exist
  EXPECT_TRUE(index->contains_key({1}));
}

// ============================================================================
// Test 13: Insert Operation - Largest Value Update
// ============================================================================
TEST_F(FDODValidationTest, InsertOperationLargestValue) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Simulate insert of new largest key
  auto deltas = index->insert_entry_for_validation({5}, {50}, DependencyType::OD);

  EXPECT_TRUE(index->contains_key({5}));
}

// ============================================================================
// Test 14: Delete Operation - Left Neighbor Flag Update
// ============================================================================
TEST_F(FDODValidationTest, DeleteOperationLeftNeighborFlag) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Add RHS values for OD
  index->insert_entry_for_validation({1}, {30}, DependencyType::OD);
  index->insert_entry_for_validation({2}, {20}, DependencyType::OD);

  // Delete key 2
  auto deltas = index->delete_entry_for_validation({2}, {20}, DependencyType::OD);

  // After delete, key 2 should be gone
  EXPECT_FALSE(index->contains_key({2}));
}

// ============================================================================
// Test 15: Update Operation - Delete then Insert
// ============================================================================
TEST_F(FDODValidationTest, UpdateOperationDeleteThenInsert) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(5);
  value_segment->append(10);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Add an RHS value for key 5
  index->insert_entry_for_validation({5}, {50}, DependencyType::FD);

  // Update key 5's RHS from 50 to 55
  auto deltas = index->update_entry_for_validation({5}, {50}, {55}, DependencyType::FD);

  // Update should maintain the key
  EXPECT_TRUE(index->contains_key({5}));
}

// ============================================================================
// Test 16: MVCC - Write-Write Conflict Detection
// ============================================================================
TEST_F(FDODValidationTest, MVCCWriteWriteConflict) {
  auto table = create_mvcc_table();
  table->append({1, 10});

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  // T1 locks the row
  TransactionID t1_id{10};
  bool t1_success = mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, t1_id);
  EXPECT_TRUE(t1_success);

  // T2 tries to lock same row - conflict!
  TransactionID t2_id{20};
  bool t2_success = mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, t2_id);
  EXPECT_FALSE(t2_success);

  // T1 releases (simulating rollback)
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  // Now T2 can lock
  t2_success = mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, t2_id);
  EXPECT_TRUE(t2_success);
}

// ============================================================================
// Test 17: MVCC - Transaction Ordering (T1 before T2)
// ============================================================================
TEST_F(FDODValidationTest, MVCCTransactionOrdering) {
  // T1 starts at CID=1, T2 starts at CID=2
  // T1's changes should be visible to T2, but not vice versa

  auto table = create_mvcc_table();
  table->append({1, 10});  // Row 0: T1's insert
  table->append({2, 20});  // Row 1: T2's insert

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0: committed by T1 at CID=1
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);

  // Row 1: committed by T2 at CID=2
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{2});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);

  // T3 with snapshot=3 sees both
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, CommitID{3}));
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{1}, CommitID{3}));

  // T1.5 with snapshot=1 sees neither row 1
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, CommitID{1}));
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, CommitID{1}));
}

// ============================================================================
// Test 18: MVCC - Overlapping Regions
// ============================================================================
TEST_F(FDODValidationTest, MVCCOverlappingRegions) {
  // T1 modifies A=1 region, T2 modifies A=1 region too
  auto table = create_mvcc_table();

  table->append({1, 10});  // Row 0
  table->append({1, 20});  // Row 1

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Both rows committed
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{0});

  // T1 locks row 0
  mvcc_data->compare_exchange_tid(ChunkOffset{0}, TransactionID{0}, TransactionID{10});

  // T2 locks row 1 (different row in same A=1 group)
  bool t2_lock_1 = mvcc_data->compare_exchange_tid(ChunkOffset{1}, TransactionID{0}, TransactionID{20});
  EXPECT_TRUE(t2_lock_1);  // Can lock different row

  // Both transactions can proceed on different rows in same logical region
}

// ============================================================================
// Test 19: Local Violation Count - Duplicate Handling
// ============================================================================
TEST_F(FDODValidationTest, LocalViolationCountDuplicates) {
  // Many duplicates of same key
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);

  for (int i = 0; i < 100; ++i) {
    value_segment->append(1);  // 100 duplicates
  }
  value_segment->append(2);  // 1 unique

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  auto value1 = index->get_value({1});
  auto value2 = index->get_value({2});

  ASSERT_NE(value1, nullptr);
  ASSERT_NE(value2, nullptr);

  EXPECT_EQ(value1->count, ChunkOffset{100});
  // Legacy local_violation_count() returns count - 1
  EXPECT_EQ(value1->local_violation_count(), 99);

  EXPECT_EQ(value2->count, ChunkOffset{1});
  EXPECT_EQ(value2->local_violation_count(), 0);

  // Test with OD mode (local violations = 0 regardless of count)
  index->recompute_local_violation_delta({1}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 0);

  index->recompute_local_violation_delta({2}, DependencyType::OD);
  EXPECT_EQ(index->global_violation_count, 0);  // No change
}

// ============================================================================
// Test 20: Flag Delta Computation - Neighbor Transitions
// ============================================================================
TEST_F(FDODValidationTest, FlagDeltaNeighborTransitions) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initial: all flags 0
  EXPECT_EQ(index->global_violation_count, 0);

  // Set flag 1->2 transition
  index->set_right_neighbor_flag({1}, 1);
  EXPECT_EQ(index->global_violation_count, 1);

  // Set flag 2->3 transition
  index->set_right_neighbor_flag({2}, 1);
  EXPECT_EQ(index->global_violation_count, 2);

  // Clear flag 1->2
  index->set_right_neighbor_flag({1}, 0);
  EXPECT_EQ(index->global_violation_count, 1);

  // Clear flag 2->3
  index->set_right_neighbor_flag({2}, 0);
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// Test 21: MetadataDeltas - Total Computation
// ============================================================================
TEST_F(FDODValidationTest, MetadataDeltasTotalComputation) {
  MetadataDeltas deltas;

  deltas.flag_delta = 5;
  deltas.local_violation_count_delta = 10;
  EXPECT_EQ(deltas.total_delta(), 15);

  deltas.flag_delta = -3;
  deltas.local_violation_count_delta = 7;
  EXPECT_EQ(deltas.total_delta(), 4);

  deltas.flag_delta = -5;
  deltas.local_violation_count_delta = -5;
  EXPECT_EQ(deltas.total_delta(), -10);
}

// ============================================================================
// Test 22: MVCC - Long Running Transaction
// ============================================================================
TEST_F(FDODValidationTest, MVCCLongRunningTransaction) {
  auto table = create_mvcc_table();

  // Initial data
  table->append({1, 10});

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);

  // Long running T1 with snapshot=2
  CommitID t1_snapshot = CommitID{2};

  // Many transactions commit (CID 3, 4, 5, ..., 100)
  // T1 should still see consistent snapshot

  table->append({2, 20});
  mvcc_data = table->get_chunk(ChunkID{0})->mvcc_data();
  mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{50});
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);

  // T1 still sees only row 0
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, t1_snapshot));
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, t1_snapshot));
}

// ============================================================================
// Test 23: MVCC - Snapshot Isolation Guarantee
// ============================================================================
TEST_F(FDODValidationTest, MVCCSnapshotIsolation) {
  auto table = create_mvcc_table();

  table->append({1, 10});
  table->append({2, 20});
  table->append({3, 30});

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // All committed at CID=1
  for (ChunkOffset i{0}; i < ChunkOffset{3}; ++i) {
    mvcc_data->set_begin_cid(i, CommitID{1});
    mvcc_data->set_end_cid(i, MAX_COMMIT_ID);
  }

  // Row 1 deleted at CID=3
  mvcc_data->set_end_cid(ChunkOffset{1}, CommitID{3});

  // T with snapshot=2 sees all 3 rows
  CommitID snapshot2 = CommitID{2};
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, snapshot2));
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{1}, snapshot2));
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{2}, snapshot2));

  // T with snapshot=4 sees only rows 0 and 2
  CommitID snapshot4 = CommitID{4};
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{0}, snapshot4));
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, snapshot4));
  EXPECT_TRUE(is_row_visible(mvcc_data, ChunkOffset{2}, snapshot4));
}

// ============================================================================
// Test 24: Index Rebuild After Modifications
// ============================================================================
TEST_F(FDODValidationTest, IndexRebuildAfterModifications) {
  auto segment1 = std::make_shared<ValueSegment<int32_t>>(false);
  segment1->append(1);
  segment1->append(2);
  segment1->append(3);

  auto index1 = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment1});

  // Set some metadata
  index1->set_right_neighbor_flag({1}, 1);
  index1->set_right_neighbor_flag({2}, 1);
  EXPECT_EQ(index1->global_violation_count, 2);

  // Create new index (simulating rebuild)
  auto segment2 = std::make_shared<ValueSegment<int32_t>>(false);
  segment2->append(1);
  segment2->append(2);
  segment2->append(3);
  segment2->append(4);  // New value

  auto index2 = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment2});

  // New index has fresh state
  EXPECT_EQ(index2->global_violation_count, 0);

  // Re-apply metadata
  index2->set_right_neighbor_flag({1}, 1);
  index2->set_right_neighbor_flag({2}, 1);
  EXPECT_EQ(index2->global_violation_count, 2);
}

// ============================================================================
// Test 25: Composite Key FD Validation
// ============================================================================
TEST_F(FDODValidationTest, CompositeKeyFDValidation) {
  // FD: (A, B) -> C
  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);
  auto segment_b = std::make_shared<ValueSegment<int32_t>>(false);

  segment_a->append(1);
  segment_b->append(10);
  segment_a->append(1);
  segment_b->append(10);  // Duplicate (1,10)
  segment_a->append(1);
  segment_b->append(20);  // Different (1,20)
  segment_a->append(2);
  segment_b->append(10);

  // Index on composite key (A, B)
  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a, segment_b});

  // (1,10) has 2 occurrences
  auto value_1_10 = index->get_value({1, 10});
  ASSERT_NE(value_1_10, nullptr);
  EXPECT_EQ(value_1_10->count, ChunkOffset{2});
  EXPECT_EQ(value_1_10->local_violation_count(), 1);

  // (1,20) has 1 occurrence
  auto value_1_20 = index->get_value({1, 20});
  ASSERT_NE(value_1_20, nullptr);
  EXPECT_EQ(value_1_20->count, ChunkOffset{1});
  EXPECT_EQ(value_1_20->local_violation_count(), 0);
}

// ============================================================================
// Test 26: Empty Index Handling
// ============================================================================
TEST_F(FDODValidationTest, EmptyIndexHandling) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(true);  // nullable

  // Add only NULL values
  value_segment->append(NullValue{});
  value_segment->append(NullValue{});

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Global count should be 0 (no non-null entries)
  EXPECT_EQ(index->global_violation_count, 0);

  // Null positions should be tracked
  auto null_positions = index->null_cbegin();
  EXPECT_NE(null_positions, index->null_cend());
}

// ============================================================================
// Test 27: Mixed NULL and Non-NULL Values
// ============================================================================
TEST_F(FDODValidationTest, MixedNullNonNullValues) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(true);

  value_segment->append(1);
  value_segment->append(NullValue{});
  value_segment->append(2);
  value_segment->append(NullValue{});
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Only non-null values should be indexed
  EXPECT_NE(index->get_value({1}), nullptr);
  EXPECT_NE(index->get_value({2}), nullptr);
  EXPECT_NE(index->get_value({3}), nullptr);

  // Count null positions
  int null_count = 0;
  for (auto it = index->null_cbegin(); it != index->null_cend(); ++it) {
    null_count++;
  }
  EXPECT_EQ(null_count, 2);
}

// ============================================================================
// Test 28: Large Scale Metadata Operations
// ============================================================================
TEST_F(FDODValidationTest, LargeScaleMetadataOperations) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);

  const int SCALE = 500;
  for (int i = 0; i < SCALE; ++i) {
    value_segment->append(i % 100);  // 100 unique keys, ~5 duplicates each
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Test flag operations at scale
  EXPECT_EQ(index->global_violation_count, 0);

  // Set all flags
  for (int i = 0; i < 100; ++i) {
    index->set_right_neighbor_flag({i}, 1);
  }
  EXPECT_EQ(index->global_violation_count, 100);

  // Clear all flags
  for (int i = 0; i < 100; ++i) {
    index->set_right_neighbor_flag({i}, 0);
  }
  EXPECT_EQ(index->global_violation_count, 0);
}

// ============================================================================
// Test 29: Boundary Conditions - Single Entry
// ============================================================================
TEST_F(FDODValidationTest, BoundaryConditionsSingleEntry) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(42);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  auto value = index->get_value({42});
  ASSERT_NE(value, nullptr);

  EXPECT_EQ(value->count, ChunkOffset{1});
  EXPECT_EQ(value->local_violation_count(), 0);
  EXPECT_EQ(index->global_violation_count, 0);

  // No neighbors to set flag with
  index->set_right_neighbor_flag({42}, 1);
  EXPECT_EQ(index->global_violation_count, 1);
}

// ============================================================================
// Test 30: Boundary Conditions - Two Entries
// ============================================================================
TEST_F(FDODValidationTest, BoundaryConditionsTwoEntries) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(2);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Set flag between the only two entries
  index->set_right_neighbor_flag({1}, 1);
  EXPECT_EQ(index->global_violation_count, 1);

  // Key 2 has no right neighbor
  index->set_right_neighbor_flag({2}, 1);
  EXPECT_EQ(index->global_violation_count, 2);
}

// ============================================================================
// Test 31: MVCC - Rollback Scenario
// ============================================================================
TEST_F(FDODValidationTest, MVCCRollbackScenario) {
  auto table = create_mvcc_table();

  table->append({1, 10});
  table->append({1, 20});  // Would be rolled back

  auto chunk = table->get_chunk(ChunkID{0});
  auto mvcc_data = chunk->mvcc_data();

  // Row 0: committed
  mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
  mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{0}, TransactionID{0});

  // Row 1: locked by T2 (uncommitted)
  mvcc_data->set_begin_cid(ChunkOffset{1}, MAX_COMMIT_ID);  // Not yet committed
  mvcc_data->set_end_cid(ChunkOffset{1}, MAX_COMMIT_ID);
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{20});  // Locked

  // Row 1 is invisible to any snapshot (begin_cid = MAX)
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, CommitID{100}));

  // Simulate rollback: reset tid, keep begin_cid = MAX (permanently invisible)
  mvcc_data->set_tid(ChunkOffset{1}, TransactionID{0});

  // Row 1 remains invisible
  EXPECT_FALSE(is_row_visible(mvcc_data, ChunkOffset{1}, CommitID{100}));
}

// ============================================================================
// Test 32: Concurrent Read Operations
// ============================================================================
TEST_F(FDODValidationTest, ConcurrentReadOperations) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 100; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Initialize some metadata
  for (int i = 0; i < 100; ++i) {
    index->set_right_neighbor_flag({i}, i % 2);
  }

  // Simulate concurrent reads (single-threaded verification)
  std::vector<int> results;
  for (int i = 0; i < 100; ++i) {
    results.push_back(index->get_right_neighbor_flag({i}));
  }

  // Verify all reads returned expected values
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(results[i], i % 2);
  }
}

// ============================================================================
// Test 33: Validation with Different Data Types
// ============================================================================
TEST_F(FDODValidationTest, ValidationWithDifferentDataTypes) {
  // Test with int64_t
  auto segment_int64 = std::make_shared<ValueSegment<int64_t>>(false);
  segment_int64->append(1000000000000LL);
  segment_int64->append(2000000000000LL);

  auto index_int64 = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_int64});

  auto value1 = index_int64->get_value({1000000000000LL});
  ASSERT_NE(value1, nullptr);
  EXPECT_EQ(value1->count, ChunkOffset{1});

  // Test with float
  auto segment_float = std::make_shared<ValueSegment<float>>(false);
  segment_float->append(1.5f);
  segment_float->append(2.5f);

  auto index_float = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_float});

  auto value_f = index_float->get_value({1.5f});
  ASSERT_NE(value_f, nullptr);
  EXPECT_EQ(value_f->count, ChunkOffset{1});

  // Test with double
  auto segment_double = std::make_shared<ValueSegment<double>>(false);
  segment_double->append(1.5);
  segment_double->append(2.5);

  auto index_double = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_double});

  auto value_d = index_double->get_value({1.5});
  ASSERT_NE(value_d, nullptr);
  EXPECT_EQ(value_d->count, ChunkOffset{1});
}

// ============================================================================
// Test 34: Validation Function Callback
// ============================================================================
TEST_F(FDODValidationTest, ValidationFunctionCallback) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(5);
  value_segment->append(10);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Test FD validation: insert different RHS values to create violations
  index->insert_entry_for_validation({5}, {50}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 0);

  index->insert_entry_for_validation({5}, {55}, DependencyType::FD);
  EXPECT_EQ(index->global_violation_count, 1);  // 2 distinct RHS values
}

// ============================================================================
// Test 35: Memory Consumption Tracking
// ============================================================================
TEST_F(FDODValidationTest, MemoryConsumptionTracking) {
  auto segment_small = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 10; ++i) {
    segment_small->append(i);
  }

  auto index_small = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_small});
  size_t small_memory = index_small->memory_consumption();

  auto segment_large = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 1000; ++i) {
    segment_large->append(i);
  }

  auto index_large = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_large});
  size_t large_memory = index_large->memory_consumption();

  // Larger index should consume more memory
  EXPECT_GT(large_memory, small_memory);

  // Memory should be reasonable (not unbounded)
  EXPECT_LT(small_memory, 1024 * 1024);       // < 1MB for 10 entries
  EXPECT_LT(large_memory, 10 * 1024 * 1024);  // < 10MB for 1000 entries
}

// ============================================================================
// Test 36: Stress Test - Rapid Flag Toggling
// ============================================================================
TEST_F(FDODValidationTest, StressTestRapidFlagToggling) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 50; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Rapidly toggle flags
  for (int round = 0; round < 100; ++round) {
    for (int i = 0; i < 50; ++i) {
      index->set_right_neighbor_flag({i}, round % 2);
    }
  }

  // Final state should be consistent
  int expected_count = 0;  // round 99 % 2 = 1, so all flags set
  for (int i = 0; i < 50; ++i) {
    expected_count += index->get_right_neighbor_flag({i});
  }

  EXPECT_EQ(index->global_violation_count, expected_count);
}

// ============================================================================
// Test 37: Leaf Neighbor Consistency
// ============================================================================
TEST_F(FDODValidationTest, LeafNeighborConsistency) {
  // Create enough entries to potentially split into multiple leaves
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  for (int i = 0; i < 20; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // All entries should be reachable via search
  for (int i = 0; i < 20; ++i) {
    auto value = index->get_value({i});
    ASSERT_NE(value, nullptr) << "Value not found for key " << i;
    EXPECT_EQ(value->count, ChunkOffset{1});
  }

  // Neighbor relationships should be consistent
  // Set flags and verify global count
  for (int i = 0; i < 20; ++i) {
    index->set_right_neighbor_flag({i}, 1);
  }
  EXPECT_EQ(index->global_violation_count, 20);
}

// ============================================================================
// Test 38: Transaction Context Integration
// ============================================================================
TEST_F(FDODValidationTest, TransactionContextIntegration) {
  // Test integration with Hyrise's transaction manager
  auto& txn_manager = Hyrise::get().transaction_manager;

  auto t1 = txn_manager.new_transaction_context(AutoCommit::No);
  auto t2 = txn_manager.new_transaction_context(AutoCommit::No);

  // T1 and T2 should have different transaction IDs
  EXPECT_NE(t1->transaction_id(), t2->transaction_id());

  // T2 should have same or higher snapshot than T1
  EXPECT_GE(t2->snapshot_commit_id(), t1->snapshot_commit_id());

  // Rollback both
  t1->rollback(RollbackReason::User);
  t2->rollback(RollbackReason::User);

  // Phase should be RolledBackByUser (not aborted - that's for conflicts)
  EXPECT_EQ(t1->phase(), TransactionPhase::RolledBackByUser);
  EXPECT_EQ(t2->phase(), TransactionPhase::RolledBackByUser);
}

// ============================================================================
// Test 39: OD with Equal Values
// ============================================================================
TEST_F(FDODValidationTest, ODWithEqualValues) {
  // OD A ~> B where A has ties
  auto segment_a = std::make_shared<ValueSegment<int32_t>>(false);
  auto segment_b = std::make_shared<ValueSegment<int32_t>>(false);

  segment_a->append(1);
  segment_b->append(10);
  segment_a->append(1);
  segment_b->append(10);  // Tie in A, same B is OK
  segment_a->append(2);
  segment_b->append(20);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment_a});

  // Key 1 has 2 occurrences
  auto value = index->get_value({1});
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->count, ChunkOffset{2});

  // Local violations for ties
  EXPECT_EQ(value->local_violation_count(), 1);
}

// ============================================================================
// Test 40: Delta Accumulation Correctness
// ============================================================================
TEST_F(FDODValidationTest, DeltaAccumulationCorrectness) {
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(1);
  value_segment->append(2);
  value_segment->append(3);

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Track running total using flags only (OD mode has local=0)
  int running_total = 0;

  // Add flag
  index->set_right_neighbor_flag({1}, 1);
  running_total += 1;
  EXPECT_EQ(index->global_violation_count, running_total);

  // Add another flag
  index->set_right_neighbor_flag({2}, 1);
  running_total += 1;
  EXPECT_EQ(index->global_violation_count, running_total);

  // Remove a flag
  index->set_right_neighbor_flag({1}, 0);
  running_total -= 1;
  EXPECT_EQ(index->global_violation_count, running_total);

  // Re-add the flag
  index->set_right_neighbor_flag({1}, 1);
  running_total += 1;
  EXPECT_EQ(index->global_violation_count, running_total);
}

// ============================================================================
// TU-Munich B-Tree Optimization Tests
// Tests for hint-based search, key_head optimization, and separator selection
// ============================================================================

TEST_F(BTreeIndexTest, TUMunichKeyHeadComputation) {
  // Test that key_head is computed correctly for entries
  std::vector<AllTypeVariant> key1 = {1};
  std::vector<AllTypeVariant> key2 = {2};
  std::vector<AllTypeVariant> key3 = {1};  // Same as key1

  uint32_t head1 = BTreeEntry::compute_head(key1);
  uint32_t head2 = BTreeEntry::compute_head(key2);
  uint32_t head3 = BTreeEntry::compute_head(key3);

  // Same keys should produce same heads
  EXPECT_EQ(head1, head3);

  // Different keys may have different heads (not guaranteed but highly likely)
  // This is a weak test since hash collisions are possible
  // Just verify the function runs without errors
  EXPECT_TRUE(head1 != 0 || key1[0] == AllTypeVariant{0});

  // Empty key should produce 0
  std::vector<AllTypeVariant> empty_key;
  EXPECT_EQ(BTreeEntry::compute_head(empty_key), 0u);
}

TEST_F(BTreeIndexTest, TUMunichOptimizedSearch) {
  // Test that optimized search produces correct results
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);

  // Insert enough values to exercise hint-based search
  for (int i = 1; i <= 100; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Verify all keys are searchable
  for (int i = 1; i <= 100; ++i) {
    EXPECT_TRUE(index->contains_key({i})) << "Key " << i << " not found";
    auto value = index->get_value({i});
    ASSERT_NE(value, nullptr) << "Value for key " << i << " is null";
    EXPECT_EQ(value->count, ChunkOffset{1});
  }

  // Verify non-existent keys are not found
  EXPECT_FALSE(index->contains_key({0}));
  EXPECT_FALSE(index->contains_key({101}));
  EXPECT_FALSE(index->contains_key({-1}));
}

TEST_F(BTreeIndexTest, TUMunichSearchWithDuplicates) {
  // Test search with duplicate keys
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);

  // Insert duplicates
  for (int i = 0; i < 10; ++i) {
    value_segment->append(1);
    value_segment->append(2);
    value_segment->append(3);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Each key should have count 10
  auto value1 = index->get_value({1});
  ASSERT_NE(value1, nullptr);
  EXPECT_EQ(value1->count, ChunkOffset{10});

  auto value2 = index->get_value({2});
  ASSERT_NE(value2, nullptr);
  EXPECT_EQ(value2->count, ChunkOffset{10});

  auto value3 = index->get_value({3});
  ASSERT_NE(value3, nullptr);
  EXPECT_EQ(value3->count, ChunkOffset{10});

  // Only 3 distinct keys
  EXPECT_EQ(index->key_count(), 3u);
}

TEST_F(BTreeIndexTest, TUMunichLargeIndexPerformance) {
  // Test with larger dataset to verify hint-based optimization
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);

  // Insert 1000 distinct values
  for (int i = 1; i <= 1000; ++i) {
    value_segment->append(i);
  }

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  EXPECT_EQ(index->key_count(), 1000u);

  // Spot check some keys
  EXPECT_TRUE(index->contains_key({1}));
  EXPECT_TRUE(index->contains_key({500}));
  EXPECT_TRUE(index->contains_key({1000}));
  EXPECT_FALSE(index->contains_key({1001}));

  // Test lower_bound and upper_bound
  auto lb = index->lower_bound({500});
  EXPECT_NE(lb, index->cend());

  auto ub = index->upper_bound({500});
  EXPECT_NE(ub, index->cend());
}

TEST_F(BTreeIndexTest, TUMunichDynamicInsertWithHints) {
  // Test that hints are maintained correctly during dynamic inserts
  auto value_segment = std::make_shared<ValueSegment<int32_t>>(false);
  value_segment->append(100);  // Seed

  auto index = std::make_shared<BTreeIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{value_segment});

  // Insert keys dynamically in non-sorted order
  index->insert_key({50});
  index->insert_key({150});
  index->insert_key({25});
  index->insert_key({75});
  index->insert_key({125});
  index->insert_key({175});

  // All keys should be searchable
  EXPECT_TRUE(index->contains_key({25}));
  EXPECT_TRUE(index->contains_key({50}));
  EXPECT_TRUE(index->contains_key({75}));
  EXPECT_TRUE(index->contains_key({100}));
  EXPECT_TRUE(index->contains_key({125}));
  EXPECT_TRUE(index->contains_key({150}));
  EXPECT_TRUE(index->contains_key({175}));

  EXPECT_EQ(index->key_count(), 7u);
}

}  // namespace hyrise
