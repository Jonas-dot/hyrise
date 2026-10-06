#include <memory>
#include <vector>

#include "base_test.hpp"
#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/dependency_validation/dependency_validation_bootstrap.hpp"
#include "storage/dependency_validation/dv_tree_access.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

// Builds a small MVCC table whose rows were committed at known CIDs so the
// snapshot scan can be exercised deterministically:
//   row 0: (1, "a") committed at CID 1
//   row 1: (1, "b") committed at CID 3   -> together with row 0 an FD violation
//   row 2: (2, "c") committed at CID 2
class DependencyValidationBootstrapTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    table =
        std::make_shared<Table>(TableColumnDefinitions{{"lhs", DataType::Int, false}, {"rhs", DataType::String, false}},
                                TableType::Data, ChunkOffset{100}, UseMvcc::Yes);
    table->append({int32_t{1}, pmr_string{"a"}});
    table->append({int32_t{1}, pmr_string{"b"}});
    table->append({int32_t{2}, pmr_string{"c"}});

    auto mvcc_data = table->get_chunk(ChunkID{0})->mvcc_data();
    mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{1});
    mvcc_data->set_begin_cid(ChunkOffset{1}, CommitID{3});
    mvcc_data->set_begin_cid(ChunkOffset{2}, CommitID{2});
    for (auto offset = ChunkOffset{0}; offset < ChunkOffset{3}; ++offset) {
      mvcc_data->set_end_cid(offset, MAX_COMMIT_ID);
    }

    spec = DependencySpec{.kind = DependencyKind::FD,
                          .lhs_column_ids = {ColumnID{0}},
                          .rhs_column_ids = {ColumnID{1}},
                          .lhs_column_types = {DataType::Int},
                          .rhs_column_types = {DataType::String}};
  }

  std::shared_ptr<Table> table;
  DependencySpec spec;
};

TEST_F(DependencyValidationBootstrapTest, ScanIncludesOnlyRowsVisibleAtSnapshot) {
  // Each higher build CID admits exactly one more committed row; no snapshot
  // ever sees a row committed later than itself.
  EXPECT_EQ(scan_visible_dependency_rows(*table, spec, CommitID{1}).size(), 1u);
  EXPECT_EQ(scan_visible_dependency_rows(*table, spec, CommitID{2}).size(), 2u);
  EXPECT_EQ(scan_visible_dependency_rows(*table, spec, CommitID{3}).size(), 3u);
}

TEST_F(DependencyValidationBootstrapTest, BatchFdOracleCountsDistinctRhsPerLhs) {
  // At CID 2 the LHS values 1 and 2 are distinct -> no violation. At CID 3 the
  // LHS value 1 maps to both "a" and "b" -> one violation.
  EXPECT_EQ(batch_fd_violation_count(scan_visible_dependency_rows(*table, spec, CommitID{2})), 0);
  EXPECT_EQ(batch_fd_violation_count(scan_visible_dependency_rows(*table, spec, CommitID{3})), 1);
}

TEST_F(DependencyValidationBootstrapTest, BatchOdOracleCountsLocalAndNeighborViolations) {
  // At CID 3, LHS 1 has two RHS values ("a", "b"), contributing one local
  // violation. Its largest RHS ("b") is before LHS 2's smallest RHS ("c"),
  // so there is no neighbor violation yet.
  const auto rows = scan_visible_dependency_rows(*table, spec, CommitID{3});
  EXPECT_EQ(batch_od_violation_count(rows), 1);

  // Replace the second group with a smaller RHS. The same local ambiguity now
  // additionally violates the ordered boundary: max(1) = "b" > min(2) = "a".
  auto neighbor_rows = rows;
  neighbor_rows.back().second = rows.front().second;
  EXPECT_EQ(batch_od_violation_count(neighbor_rows), 2);
}

TEST_F(DependencyValidationBootstrapTest, BuildReflectsExactlyTheBuildSnapshot) {
  const auto tree_at_two = build_dependency_validator(*table, spec, CommitID{2});
  EXPECT_TRUE(DVTreeAccess::holds_at(*tree_at_two, CommitID{2}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree_at_two, CommitID{2}), 0);

  const auto tree_at_three = build_dependency_validator(*table, spec, CommitID{3});
  EXPECT_FALSE(DVTreeAccess::holds_at(*tree_at_three, CommitID{3}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree_at_three, CommitID{3}), 1);
}

TEST_F(DependencyValidationBootstrapTest, OdBuildMatchesIndependentOracle) {
  auto od_spec = spec;
  od_spec.kind = DependencyKind::OD;

  const auto rows = scan_visible_dependency_rows(*table, od_spec, CommitID{3});
  const auto tree = build_dependency_validator(*table, od_spec, CommitID{3});
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{3}), batch_od_violation_count(rows));
}

TEST_F(DependencyValidationBootstrapTest, BuildExcludesRowsDeletedAtOrBeforeTheBuildSnapshot) {
  // Delete row 0 (LHS 1 -> "a") at CID 3. At the CID-3 snapshot only rows 1 and
  // 2 survive, so LHS 1 maps to a single RHS ("b") and the FD holds again.
  table->get_chunk(ChunkID{0})->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{3});

  EXPECT_EQ(scan_visible_dependency_rows(*table, spec, CommitID{3}).size(), 2u);

  const auto tree = build_dependency_validator(*table, spec, CommitID{3});
  EXPECT_TRUE(DVTreeAccess::holds_at(*tree, CommitID{3}));
  EXPECT_EQ(DVTreeAccess::violation_count_at(*tree, CommitID{3}), 0);
}

TEST_F(DependencyValidationBootstrapTest, TableBuildAndAttachProducesQueryableValidator) {
  // This test exercises the Hyrise-facing API, whose snapshots must not exceed
  // TransactionManager's global watermark. Make all fixture rows visible at
  // the real current CID; the earlier tests cover synthetic CIDs directly on
  // the private bootstrap/tree API.
  const auto build_cid = Hyrise::get().transaction_manager.last_commit_id();
  const auto mvcc_data = table->get_chunk(ChunkID{0})->mvcc_data();
  for (auto offset = ChunkOffset{0}; offset < ChunkOffset{3}; ++offset) {
    mvcc_data->set_begin_cid(offset, build_cid);
  }
  table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, DependencyKind::FD, build_cid);

  const auto api = table->dependency_validation_api();
  ASSERT_EQ(api.size(), 1u);
  const auto& validator = api.front();

  EXPECT_EQ(validator.kind(), DependencyKind::FD);
  EXPECT_EQ(validator.visible_commit_id(), build_cid);
  EXPECT_FALSE(validator.holds_at(build_cid));
  EXPECT_EQ(validator.violation_count_at(build_cid), 1);
}

TEST_F(DependencyValidationBootstrapTest, FailedBuildLeavesNoEmptyDescriptorAttached) {
  EXPECT_THROW(
      table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, DependencyKind::FD, UNSET_COMMIT_ID),
      std::logic_error);
  EXPECT_TRUE(table->dependency_validation_api().empty());
}

}  // namespace hyrise::dv_tree
