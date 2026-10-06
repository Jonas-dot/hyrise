#include <future>
#include <memory>
#include <new>

#include "base_test.hpp"
#include "storage/dependency_validation/dependency_validation_commit_coordinator.hpp"
#include "storage/dependency_validation/dv_tree_access.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

class DependencyValidationCommitCoordinatorTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    table = std::make_shared<Table>(
        TableColumnDefinitions{{"lhs", DataType::Int, false}, {"rhs", DataType::String, false}}, TableType::Data);
    tree = std::make_shared<DVTree>(DependencyKind::FD);
  }

  DependencyValidationWriteSet write_set_for(const std::string& lhs, const std::string& rhs) const {
    auto write_set = DependencyValidationWriteSet{};
    write_set.insert(table, tree, "FD [0] -> [1]", lhs, rhs);
    return write_set;
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<DVTree> tree;
};

// Hyrise's INITIAL_COMMIT_ID (1) is the baseline snapshot, not a transaction
// commit: TransactionManager hands the first committing transaction CID 2, so
// the coordinator's registration frontier starts at INITIAL_COMMIT_ID + 1.
// These tests therefore use externally assigned Hyrise CIDs starting at 2.
TEST_F(DependencyValidationCommitCoordinatorTest, InstallsBeforeAndPublishesWithTheHyriseCid) {
  auto coordinator = DependencyValidationCommitCoordinator{};
  coordinator.reserve_commit_id(CommitID{2});

  const auto write_set = write_set_for("lhs", "rhs");
  auto commit = coordinator.register_commit(CommitID{2}, &write_set);
  ASSERT_NE(commit, nullptr);

  commit->seal_and_wait_until_applied();
  EXPECT_THROW(DVTreeAccess::holds_at(*tree, CommitID{2}), SnapshotNotVisible);

  commit->publish_visibility();
  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{2}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{2}), 0);
}

TEST_F(DependencyValidationCommitCoordinatorTest, HigherCidWaitsForLateLowerFootprintRegistration) {
  auto coordinator = DependencyValidationCommitCoordinator{};
  coordinator.reserve_commit_id(CommitID{2});
  coordinator.reserve_commit_id(CommitID{3});

  const auto lower_write_set = write_set_for("shared", "rhs-a");
  const auto higher_write_set = write_set_for("shared", "rhs-b");

  auto higher_registration = std::async(std::launch::async, [&] {
    return coordinator.register_commit(CommitID{3}, &higher_write_set);
  });

  const auto lower_commit = coordinator.register_commit(CommitID{2}, &lower_write_set);
  ASSERT_NE(lower_commit, nullptr);
  lower_commit->seal_and_wait_until_applied();
  lower_commit->publish_visibility();

  auto higher_commit = higher_registration.get();
  ASSERT_NE(higher_commit, nullptr);
  higher_commit->seal_and_wait_until_applied();
  higher_commit->publish_visibility();

  EXPECT_FALSE(DVTreeAccess::holds_at(*tree, CommitID{3}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{3}), 1);
}

TEST_F(DependencyValidationCommitCoordinatorTest, EmptyRegistrationLeavesNoTicketButUnblocksLaterCid) {
  auto coordinator = DependencyValidationCommitCoordinator{};
  coordinator.reserve_commit_id(CommitID{2});
  coordinator.reserve_commit_id(CommitID{3});

  EXPECT_EQ(coordinator.register_commit(CommitID{2}, nullptr), nullptr);

  const auto write_set = write_set_for("lhs", "rhs");
  auto commit = coordinator.register_commit(CommitID{3}, &write_set);
  ASSERT_NE(commit, nullptr);
  commit->seal_and_wait_until_applied();
  commit->publish_visibility();

  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{3}));
}

TEST_F(DependencyValidationCommitCoordinatorTest, AbortedPreRowTicketDoesNotStrandTheNextCid) {
  auto coordinator = DependencyValidationCommitCoordinator{};
  coordinator.reserve_commit_id(CommitID{2});
  coordinator.reserve_commit_id(CommitID{3});

  const auto failed_write_set = write_set_for("lhs", "rhs-a");
  auto failed_commit = coordinator.register_commit(CommitID{2}, &failed_write_set);
  ASSERT_NE(failed_commit, nullptr);
  EXPECT_TRUE(failed_commit->abort_before_row_commit());

  const auto later_write_set = write_set_for("lhs", "rhs-b");
  auto later_commit = coordinator.register_commit(CommitID{3}, &later_write_set);
  ASSERT_NE(later_commit, nullptr);
  later_commit->seal_and_wait_until_applied();
  later_commit->publish_visibility();

  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{3}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{3}), 0);
}

TEST_F(DependencyValidationCommitCoordinatorTest, TicketCreationFailureRetiresCidAndUnblocksNextCid) {
  auto fail_next_creation = true;
  auto coordinator = DependencyValidationCommitCoordinator{[&] {
    if (fail_next_creation) {
      fail_next_creation = false;
      throw std::bad_alloc{};
    }
  }};
  const auto tree = std::make_shared<DVTree>(DependencyKind::FD, INITIAL_COMMIT_ID);

  auto failed_write_set = DependencyValidationWriteSet{};
  failed_write_set.insert(
      std::make_shared<Table>(TableColumnDefinitions{{"value", DataType::Int, false}}, TableType::Data), tree, "fd",
      "k", "a");
  auto later_write_set = DependencyValidationWriteSet{};
  later_write_set.insert(
      std::make_shared<Table>(TableColumnDefinitions{{"value", DataType::Int, false}}, TableType::Data), tree, "fd",
      "x", "b");

  coordinator.reserve_commit_id(CommitID{2});
  coordinator.reserve_commit_id(CommitID{3});
  EXPECT_THROW(static_cast<void>(coordinator.register_commit(CommitID{2}, &failed_write_set)), std::bad_alloc);

  auto later_commit = coordinator.register_commit(CommitID{3}, &later_write_set);
  ASSERT_TRUE(later_commit);
  later_commit->seal_and_wait_until_applied();
  later_commit->publish_visibility();

  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{3}));
}

TEST_F(DependencyValidationCommitCoordinatorTest, ActiveSnapshotPreventsSoftHistoryEviction) {
  auto retained_tree = DVTree{DependencyKind::FD, 1};

  auto first = Transaction{};
  first.insert("lhs", "rhs-a");
  EXPECT_EQ(DVTreeAccess::apply_commit(retained_tree, first), CommitID{1});
  DVTreeAccess::set_lowest_active_snapshot(retained_tree, CommitID{1});

  auto second = Transaction{};
  second.insert("lhs", "rhs-b");
  EXPECT_EQ(DVTreeAccess::apply_commit(retained_tree, second), CommitID{2});

  auto third = Transaction{};
  third.insert("lhs", "rhs-c");
  EXPECT_EQ(DVTreeAccess::apply_commit(retained_tree, third), CommitID{3});

  EXPECT_TRUE(DVTreeAccess::holds_at(retained_tree, CommitID{1}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(retained_tree, CommitID{2}), 1);
  EXPECT_EQ(DVTreeAccess::violation_count_at(retained_tree, CommitID{3}), 2);
  EXPECT_EQ(retained_tree.memory_statistics().retained_history_entries, 2u);

  DVTreeAccess::set_lowest_active_snapshot(retained_tree, CommitID{3});
  EXPECT_THROW(DVTreeAccess::violation_count_at(retained_tree, CommitID{1}), SnapshotHorizonViolation);
  EXPECT_EQ(DVTreeAccess::violation_count_at(retained_tree, CommitID{3}), 2);
}

TEST_F(DependencyValidationCommitCoordinatorTest, StalledOldestSnapshotKeepsEveryRequiredHistoryPoint) {
  // The coordinator only receives the *minimum* snapshot of all active
  // Hyrise transactions. Keeping CID 2 here therefore models an arbitrary
  // number of newer active transactions plus one deliberately stalled reader
  // at CID 2.
  tree = std::make_shared<DVTree>(DependencyKind::FD, 1);
  auto coordinator = DependencyValidationCommitCoordinator{};

  for (const auto raw_cid : {2u, 3u, 4u, 5u, 6u}) {
    const auto cid = CommitID{raw_cid};
    coordinator.reserve_commit_id(cid);
    const auto write_set = write_set_for("shared", "rhs-" + std::to_string(raw_cid));
    auto commit = coordinator.register_commit(cid, &write_set);
    ASSERT_NE(commit, nullptr);
    commit->seal_and_wait_until_applied();
    commit->publish_visibility();
    if (raw_cid == 2u)
      coordinator.update_lowest_active_snapshot(cid);
  }

  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{2}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{3}), 1);
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{4}), 2);
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{5}), 3);
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{6}), 4);
  EXPECT_GT(tree->memory_statistics().retained_history_entries, 1u);

  // Once that oldest transaction finishes, Hyrise supplies a newer minimum
  // snapshot and the soft target can be restored without changing CID 6.
  coordinator.update_lowest_active_snapshot(CommitID{6});
  EXPECT_THROW(DVTreeAccess::violation_count_at(*tree, CommitID{2}), SnapshotHorizonViolation);
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{6}), 4);
}

}  // namespace hyrise::dv_tree
