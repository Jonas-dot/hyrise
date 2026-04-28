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

TEST(BTreeOLCIndexTest, FDHoldsWhenEmpty) {
  auto idx = make_int_index({1, 2, 3});
  EXPECT_TRUE(idx->dependency_holds());
  EXPECT_EQ(idx->global_violation_count(), 0);
}

TEST(BTreeOLCIndexTest, FDHoldsWithUniqueMapping) {
  auto idx = make_int_index({1, 2, 3});
  // Insert unique LHS → RHS mappings (FD holds)
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{100}}, DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{2}}, {AllTypeVariant{200}}, DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{3}}, {AllTypeVariant{300}}, DependencyType::FD);
  EXPECT_TRUE(idx->dependency_holds());
}

TEST(BTreeOLCIndexTest, FDViolatedWithMultipleRHS) {
  auto idx = make_int_index({1, 2, 3});
  // Same LHS, different RHS → FD violation
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{100}}, DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD);
  EXPECT_FALSE(idx->dependency_holds());
  EXPECT_GT(idx->global_violation_count(), 0);
}

TEST(BTreeOLCIndexTest, FDViolationResolvedAfterDelete) {
  auto idx = make_int_index({1, 2, 3});
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{100}}, DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD);
  EXPECT_FALSE(idx->dependency_holds());

  // Remove the offending second mapping
  idx->delete_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD);
  EXPECT_TRUE(idx->dependency_holds());
}

TEST(BTreeOLCIndexTest, MultiColumnFDHoldsWithUniqueMapping) {
  auto idx = make_int_index({1, 2, 3});

  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{1000}},
                                   DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{11}}, {AllTypeVariant{101}, AllTypeVariant{1001}},
                                   DependencyType::FD);

  EXPECT_TRUE(idx->dependency_holds());
  EXPECT_EQ(idx->global_violation_count(), 0);
}

TEST(BTreeOLCIndexTest, MultiColumnFDViolationAndResolution) {
  auto idx = make_int_index({1, 2, 3});

  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{1000}},
                                   DependencyType::FD);
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{1001}},
                                   DependencyType::FD);

  EXPECT_FALSE(idx->dependency_holds());
  EXPECT_GT(idx->global_violation_count(), 0);

  idx->delete_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{1001}},
                                   DependencyType::FD);

  EXPECT_TRUE(idx->dependency_holds());
  EXPECT_EQ(idx->global_violation_count(), 0);
}

TEST(BTreeOLCIndexTest, ODHoldsWithMonotonicMapping) {
  auto idx = make_int_index({1, 2, 3});
  // OD: insert keys 1→10, 2→20, 3→30 (monotonic → no violation)
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{10}}, DependencyType::OD);
  idx->insert_entry_for_validation({AllTypeVariant{2}}, {AllTypeVariant{20}}, DependencyType::OD);
  idx->insert_entry_for_validation({AllTypeVariant{3}}, {AllTypeVariant{30}}, DependencyType::OD);
  EXPECT_TRUE(idx->dependency_holds());
}

TEST(BTreeOLCIndexTest, MultiColumnODHoldsWithMonotonicMapping) {
  auto idx = make_int_index({1, 2, 3});

  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{1}}, {AllTypeVariant{10}, AllTypeVariant{1}},
                                   DependencyType::OD);
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{2}}, {AllTypeVariant{10}, AllTypeVariant{2}},
                                   DependencyType::OD);
  idx->insert_entry_for_validation({AllTypeVariant{2}, AllTypeVariant{1}}, {AllTypeVariant{11}, AllTypeVariant{0}},
                                   DependencyType::OD);

  EXPECT_TRUE(idx->dependency_holds());
  EXPECT_EQ(idx->global_violation_count(), 0);
}

TEST(BTreeOLCIndexTest, MultiColumnODViolationAndResolution) {
  auto idx = make_int_index({1, 2, 3});

  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{1}}, {AllTypeVariant{10}, AllTypeVariant{5}},
                                   DependencyType::OD);
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{2}}, {AllTypeVariant{10}, AllTypeVariant{4}},
                                   DependencyType::OD);

  EXPECT_FALSE(idx->dependency_holds());
  EXPECT_GT(idx->global_violation_count(), 0);

  idx->delete_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{2}}, {AllTypeVariant{10}, AllTypeVariant{4}},
                                   DependencyType::OD);

  EXPECT_TRUE(idx->dependency_holds());
  EXPECT_EQ(idx->global_violation_count(), 0);
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

TEST(VersionedGHistoryTest, EmptyHistoryReturnsZero) {
  olc_detail::VersionedGHistory gh;
  EXPECT_EQ(gh.query(CommitID{0}), 0);
  EXPECT_EQ(gh.query(CommitID{100}), 0);
  EXPECT_EQ(gh.query_latest(), 0);
}

TEST(VersionedGHistoryTest, SingleCommitDelta) {
  olc_detail::VersionedGHistory gh;
  gh.update(CommitID{10}, +3);

  EXPECT_EQ(gh.query(CommitID{5}), 0);    // before commit
  EXPECT_EQ(gh.query(CommitID{10}), 3);   // at commit
  EXPECT_EQ(gh.query(CommitID{100}), 3);  // after commit
  EXPECT_EQ(gh.query_latest(), 3);
}

TEST(VersionedGHistoryTest, MultipleCommitsInOrder) {
  olc_detail::VersionedGHistory gh;
  gh.update(CommitID{10}, +1);
  gh.update(CommitID{20}, +2);
  gh.update(CommitID{30}, -1);

  EXPECT_EQ(gh.query(CommitID{5}), 0);    // before all
  EXPECT_EQ(gh.query(CommitID{10}), 1);   // after first
  EXPECT_EQ(gh.query(CommitID{15}), 1);   // between first and second
  EXPECT_EQ(gh.query(CommitID{20}), 3);   // after second (1+2)
  EXPECT_EQ(gh.query(CommitID{25}), 3);   // between second and third
  EXPECT_EQ(gh.query(CommitID{30}), 2);   // after third (1+2-1)
  EXPECT_EQ(gh.query(CommitID{99}), 2);
  EXPECT_EQ(gh.query_latest(), 2);
}

TEST(VersionedGHistoryTest, OutOfOrderCIDs) {
  olc_detail::VersionedGHistory gh;
  // Commit CID=20 arrives before CID=10.
  gh.update(CommitID{20}, +5);
  gh.update(CommitID{10}, +3);

  // Despite out-of-order arrival, snapshot queries must be correct.
  EXPECT_EQ(gh.query(CommitID{5}), 0);    // before both
  EXPECT_EQ(gh.query(CommitID{10}), 3);   // sees only CID=10
  EXPECT_EQ(gh.query(CommitID{15}), 3);   // sees only CID=10
  EXPECT_EQ(gh.query(CommitID{20}), 8);   // sees CID=10 + CID=20
  EXPECT_EQ(gh.query_latest(), 8);
}

TEST(VersionedGHistoryTest, OutOfOrderThreeCIDs) {
  olc_detail::VersionedGHistory gh;
  // Arrive in order: 30, 10, 20
  gh.update(CommitID{30}, -1);
  gh.update(CommitID{10}, +3);
  gh.update(CommitID{20}, +2);

  EXPECT_EQ(gh.query(CommitID{5}), 0);
  EXPECT_EQ(gh.query(CommitID{10}), 3);
  EXPECT_EQ(gh.query(CommitID{20}), 5);   // 3+2
  EXPECT_EQ(gh.query(CommitID{30}), 4);   // 3+2-1
  EXPECT_EQ(gh.query_latest(), 4);
}

TEST(VersionedGHistoryTest, CoalescingSameCID) {
  olc_detail::VersionedGHistory gh;
  gh.update(CommitID{10}, +1);
  gh.update(CommitID{10}, +2);
  gh.update(CommitID{10}, -1);

  EXPECT_EQ(gh.query(CommitID{10}), 2);  // 1+2-1 = 2
  EXPECT_EQ(gh.query_latest(), 2);
}

TEST(VersionedGHistoryTest, CoalescingWithInterleavedCIDs) {
  olc_detail::VersionedGHistory gh;
  // Simulates interleaved per-row updates from two transactions.
  gh.update(CommitID{10}, +1);   // T1 row 1
  gh.update(CommitID{20}, +1);   // T2 row 1
  gh.update(CommitID{10}, +1);   // T1 row 2 -- must coalesce with CID=10

  EXPECT_EQ(gh.query(CommitID{10}), 2);  // T1: +1+1
  EXPECT_EQ(gh.query(CommitID{20}), 3);  // T1+T2: 2+1
  EXPECT_EQ(gh.query_latest(), 3);
}

TEST(VersionedGHistoryTest, DeltaZeroSkipped) {
  olc_detail::VersionedGHistory gh;
  gh.update(CommitID{10}, +1);
  gh.update(CommitID{20}, 0);   // delta=0 should be a no-op
  gh.update(CommitID{30}, +1);

  EXPECT_EQ(gh.query(CommitID{20}), 1);  // no entry for CID=20
  EXPECT_EQ(gh.query(CommitID{30}), 2);
  EXPECT_EQ(gh.query_latest(), 2);
}

TEST(VersionedGHistoryTest, QueryBeforeAnyCommit) {
  olc_detail::VersionedGHistory gh;
  gh.update(CommitID{100}, +5);

  EXPECT_EQ(gh.query(CommitID{0}), 0);
  EXPECT_EQ(gh.query(CommitID{1}), 0);
  EXPECT_EQ(gh.query(CommitID{99}), 0);
}

TEST(BTreeOLCIndexTest, FDSnapshotMultipleCommits) {
  // Each commit uses a different CID; snapshot queries must see exactly
  // the violations contributed by commits <= snapshot_cid.
  auto idx = make_int_index({1, 2, 3});

  // Commit CID=10: insert (1→100) -- no violation
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{100}}, DependencyType::FD, CommitID{10});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);

  // Commit CID=20: insert (1→200) -- violation (+1)
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD, CommitID{20});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);  // snapshot before violation
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 1);  // snapshot at violation

  // Commit CID=30: delete (1→200) -- resolves violation (-1)
  idx->delete_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD, CommitID{30});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 1);  // still sees the old state
  EXPECT_EQ(idx->global_violation_count(CommitID{30}), 0);  // resolved

  EXPECT_TRUE(idx->dependency_holds(CommitID{10}));
  EXPECT_FALSE(idx->dependency_holds(CommitID{20}));
  EXPECT_TRUE(idx->dependency_holds(CommitID{30}));
}

TEST(BTreeOLCIndexTest, FDSnapshotConcurrentReaders) {
  // Realistic MVCC scenario: commits happen in CID order (as Hyrise guarantees),
  // then multiple transactions with different snapshot_cids query concurrently.
  auto idx = make_int_index({1, 2, 3});

  // Commit CID=10: insert (1→100) -- no violation
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{100}}, DependencyType::FD, CommitID{10});

  // Commit CID=20: insert (1→200) -- creates violation (2 distinct RHS for det=1)
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{200}}, DependencyType::FD, CommitID{20});

  // Commit CID=30: insert (2→300) -- different determinant, no new violation
  idx->insert_entry_for_validation({AllTypeVariant{2}}, {AllTypeVariant{300}}, DependencyType::FD, CommitID{30});

  // Now different transactions query their snapshots:
  // T_A started before CID=20 → sees no violation
  EXPECT_TRUE(idx->dependency_holds(CommitID{10}));
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);

  // T_B started after CID=20 but before CID=30 → sees the violation
  EXPECT_FALSE(idx->dependency_holds(CommitID{20}));
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 1);

  // T_C started after CID=30 → still sees the violation (not resolved)
  EXPECT_FALSE(idx->dependency_holds(CommitID{30}));
  EXPECT_EQ(idx->global_violation_count(CommitID{30}), 1);

  // Query for CIDs between commits → sees only earlier commits
  EXPECT_EQ(idx->global_violation_count(CommitID{15}), 0);  // between 10 and 20
  EXPECT_EQ(idx->global_violation_count(CommitID{25}), 1);  // between 20 and 30
}

TEST(BTreeOLCIndexTest, ODSnapshotMultipleCommits) {
  auto idx = make_int_index({1, 2, 3});

  // Commit CID=10: insert (1→10) -- no violation
  idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{10}}, DependencyType::OD, CommitID{10});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);

  // Commit CID=20: insert (2→20) -- monotonic, still no violation
  idx->insert_entry_for_validation({AllTypeVariant{2}}, {AllTypeVariant{20}}, DependencyType::OD, CommitID{20});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 0);

  // Commit CID=30: insert (3→5) -- violation! max_rhs(2)=20 > min_rhs(3)=5
  idx->insert_entry_for_validation({AllTypeVariant{3}}, {AllTypeVariant{5}}, DependencyType::OD, CommitID{30});
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 0);
  EXPECT_FALSE(idx->dependency_holds(CommitID{30}));
  EXPECT_TRUE(idx->dependency_holds(CommitID{20}));
}

TEST(BTreeOLCIndexTest, MultiColumnFDSnapshotCommits) {
  auto idx = make_int_index({1, 2, 3});

  // Multi-column FD: (1,10) → (100,1000)
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{1000}},
                                   DependencyType::FD, CommitID{10});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);

  // Same LHS, different RHS → violation
  idx->insert_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{2000}},
                                   DependencyType::FD, CommitID{20});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 1);

  // Delete the violating entry
  idx->delete_entry_for_validation({AllTypeVariant{1}, AllTypeVariant{10}}, {AllTypeVariant{100}, AllTypeVariant{2000}},
                                   DependencyType::FD, CommitID{30});
  EXPECT_EQ(idx->global_violation_count(CommitID{10}), 0);
  EXPECT_EQ(idx->global_violation_count(CommitID{20}), 1);
  EXPECT_EQ(idx->global_violation_count(CommitID{30}), 0);
}

TEST(BTreeOLCIndexTest, ConcurrentSnapshotReads) {
  auto idx = make_int_index({1, 2, 3});

  // Build up some history: 50 commits, each inserting a new distinct RHS for det=1.
  // After commit CID=k, violation count = k-1 (k distinct values → k-1 violations).
  for (int k = 1; k <= 50; ++k) {
    idx->insert_entry_for_validation({AllTypeVariant{1}}, {AllTypeVariant{k * 100}}, DependencyType::FD,
                                     CommitID{static_cast<uint32_t>(k)});
  }

  // Sanity: latest should have 49 violations.
  EXPECT_EQ(idx->global_violation_count(), 49);

  // Spawn reader threads that query various snapshots concurrently.
  constexpr int NUM_THREADS = 8;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};

  for (int t = 0; t < NUM_THREADS; ++t) {
    threads.emplace_back([&idx, &errors]() {
      for (int cid = 1; cid <= 50; ++cid) {
        int expected = cid - 1;
        int actual = idx->global_violation_count(CommitID{static_cast<uint32_t>(cid)});
        if (actual != expected) {
          errors.fetch_add(1);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(errors.load(), 0);
}

TEST(VersionedGHistoryTest, ConcurrentWritesAndReads) {
  olc_detail::VersionedGHistory gh;

  // Writer thread: commits CID 1..100, each with delta=+1.
  // Reader threads: query snapshots and verify monotonicity.
  constexpr int NUM_COMMITS = 100;
  constexpr int NUM_READERS = 4;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::thread writer([&]() {
    while (!start.load()) {}
    for (int i = 1; i <= NUM_COMMITS; ++i) {
      gh.update(CommitID{static_cast<uint32_t>(i)}, +1);
    }
  });

  std::vector<std::thread> readers;
  for (int r = 0; r < NUM_READERS; ++r) {
    readers.emplace_back([&]() {
      while (!start.load()) {}
      for (int iter = 0; iter < 500; ++iter) {
        // Query two snapshots: a lower and a higher CID.
        // The higher snapshot must have >= violations than the lower.
        int lo_g = gh.query(CommitID{30});
        int hi_g = gh.query(CommitID{80});
        if (hi_g < lo_g) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);
  writer.join();
  for (auto& th : readers) {
    th.join();
  }

  EXPECT_EQ(errors.load(), 0);
  EXPECT_EQ(gh.query(CommitID{static_cast<uint32_t>(NUM_COMMITS)}), NUM_COMMITS);
  EXPECT_EQ(gh.query_latest(), NUM_COMMITS);
}

}  // namespace hyrise