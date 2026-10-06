#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

#include "all_type_variant.hpp"
#include "gtest/gtest.h"
#include "storage/index/b_tree/b_tree_olc_index.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace hyrise {

// Build a BTreeOLCIndex over a single int32_t ValueSegment containing [values].
static std::shared_ptr<BTreeOLCIndex> make_int_index(const std::vector<int32_t>& values) {
  auto segment = std::make_shared<ValueSegment<int32_t>>();
  for (auto v : values)
    segment->append(v);
  return std::make_shared<BTreeOLCIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment});
}

// Collect all ChunkOffsets in [begin, end).
static std::vector<ChunkOffset> collect(AbstractChunkIndex::Iterator begin, AbstractChunkIndex::Iterator end) {
  return std::vector<ChunkOffset>(begin, end);
}

TEST(BTreeOLCIndexTest, ConstructionEmpty) {
  auto segment = std::make_shared<ValueSegment<int32_t>>();
  BTreeOLCIndex idx({segment});
  EXPECT_EQ(collect(idx.cbegin(), idx.cend()).size(), 0u);
}

TEST(BTreeOLCIndexTest, ConstructionSingleElement) {
  auto idx = make_int_index({42});
  auto all = collect(idx->cbegin(), idx->cend());
  ASSERT_EQ(all.size(), 1u);
  EXPECT_EQ(all[0], ChunkOffset{0});
}

TEST(BTreeOLCIndexTest, ConstructionSorted) {
  // Insert in unsorted order; index should return offsets in sorted-value order.
  // Values: 30, 10, 20  → sorted order: 10 (offset 1), 20 (offset 2), 30 (offset 0)
  auto idx = make_int_index({30, 10, 20});
  auto all = collect(idx->cbegin(), idx->cend());
  ASSERT_EQ(all.size(), 3u);
  EXPECT_EQ(all[0], ChunkOffset{1});  // value 10
  EXPECT_EQ(all[1], ChunkOffset{2});  // value 20
  EXPECT_EQ(all[2], ChunkOffset{0});  // value 30
}

TEST(BTreeOLCIndexTest, LowerBoundExact) {
  auto idx = make_int_index({10, 20, 30, 40, 50});
  // lower_bound(30) → points to the entry with value 30
  auto it = idx->lower_bound({AllTypeVariant{30}});
  ASSERT_NE(it, idx->cend());
  EXPECT_EQ(*it, ChunkOffset{2});  // 30 is at original position 2
}

TEST(BTreeOLCIndexTest, LowerBoundBetween) {
  // Values 10, 30 -- lower_bound(20) should land on 30's position
  auto idx = make_int_index({10, 30});
  auto it = idx->lower_bound({AllTypeVariant{20}});
  ASSERT_NE(it, idx->cend());
  EXPECT_EQ(*it, ChunkOffset{1});  // value 30 at offset 1
}

TEST(BTreeOLCIndexTest, LowerBoundBeforeAll) {
  auto idx = make_int_index({10, 20, 30});
  auto it = idx->lower_bound({AllTypeVariant{0}});
  EXPECT_EQ(it, idx->cbegin());
}

TEST(BTreeOLCIndexTest, LowerBoundAfterAll) {
  auto idx = make_int_index({10, 20, 30});
  auto it = idx->lower_bound({AllTypeVariant{100}});
  EXPECT_EQ(it, idx->cend());
}

TEST(BTreeOLCIndexTest, UpperBoundExact) {
  auto idx = make_int_index({10, 20, 30, 40, 50});
  auto lo = idx->lower_bound({AllTypeVariant{30}});
  auto hi = idx->upper_bound({AllTypeVariant{30}});
  auto range = collect(lo, hi);
  ASSERT_EQ(range.size(), 1u);
  EXPECT_EQ(range[0], ChunkOffset{2});
}

TEST(BTreeOLCIndexTest, UpperBoundAfterAll) {
  auto idx = make_int_index({10, 20, 30});
  auto it = idx->upper_bound({AllTypeVariant{100}});
  EXPECT_EQ(it, idx->cend());
}

TEST(BTreeOLCIndexTest, RangeQueryInclusive) {
  // Values 5, 10, 15, 20, 25 -- range [10, 20]
  auto idx = make_int_index({5, 10, 15, 20, 25});
  auto lo = idx->lower_bound({AllTypeVariant{10}});
  auto hi = idx->upper_bound({AllTypeVariant{20}});
  auto range = collect(lo, hi);
  EXPECT_EQ(range.size(), 3u);  // 10, 15, 20
}

TEST(BTreeOLCIndexTest, NullsExcluded) {
  auto segment = std::make_shared<ValueSegment<int32_t>>(/*nullable=*/true);
  segment->append(10);
  segment->append(AllTypeVariant{});  // null
  segment->append(20);

  BTreeOLCIndex idx({segment});
  auto all = collect(idx.cbegin(), idx.cend());
  EXPECT_EQ(all.size(), 2u);  // only the two non-null values

  auto nulls = collect(idx.null_cbegin(), idx.null_cend());
  EXPECT_EQ(nulls.size(), 1u);
  EXPECT_EQ(nulls[0], ChunkOffset{1});
}

TEST(BTreeOLCIndexTest, IsIndexFor) {
  auto segment = std::make_shared<ValueSegment<int32_t>>();
  segment->append(1);
  auto idx = std::make_shared<BTreeOLCIndex>(std::vector<std::shared_ptr<const AbstractSegment>>{segment});

  EXPECT_TRUE(idx->is_index_for({segment}));

  auto other = std::make_shared<ValueSegment<int32_t>>();
  EXPECT_FALSE(idx->is_index_for({other}));
}

TEST(BTreeOLCIndexTest, IndexType) {
  auto idx = make_int_index({1, 2, 3});
  EXPECT_EQ(idx->type(), ChunkIndexType::BTreeOLC);
}

TEST(BTreeOLCIndexTest, MemoryConsumptionPositive) {
  auto idx = make_int_index({1, 2, 3, 4, 5});
  EXPECT_GT(idx->memory_consumption(), 0u);
}

TEST(BTreeOLCIndexTest, EstimateMemoryConsumption) {
  size_t est = BTreeOLCIndex::estimate_memory_consumption(ChunkOffset{100}, ChunkOffset{50},
                                                          static_cast<uint32_t>(sizeof(int32_t)));
  EXPECT_GT(est, 0u);
}

TEST(BTreeOLCIndexTest, SequentialInsertLookupRange) {
  // Build index with values 0..999 inserted in order
  std::vector<int32_t> vals(1000);
  std::iota(vals.begin(), vals.end(), 0);
  auto idx = make_int_index(vals);

  // Query range [200, 299] → expect 100 hits
  auto lo = idx->lower_bound({AllTypeVariant{200}});
  auto hi = idx->upper_bound({AllTypeVariant{299}});
  auto range = collect(lo, hi);
  EXPECT_EQ(range.size(), 100u);
}

TEST(BTreeOLCIndexTest, RandomInsertLookup) {
  // Shuffled input; the index should still answer point queries correctly.
  std::vector<int32_t> vals(500);
  std::iota(vals.begin(), vals.end(), 0);
  std::mt19937 rng(42);
  std::shuffle(vals.begin(), vals.end(), rng);

  auto idx = make_int_index(vals);

  // For each value, lower_bound == upper_bound - 1
  for (int32_t v = 0; v < 500; ++v) {
    auto lo = idx->lower_bound({AllTypeVariant{v}});
    auto hi = idx->upper_bound({AllTypeVariant{v}});
    ASSERT_NE(lo, hi) << "value " << v << " not found";
    EXPECT_EQ(std::distance(lo, hi), 1);
  }
}

}  // namespace hyrise
