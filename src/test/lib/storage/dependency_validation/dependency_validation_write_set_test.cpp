#include <memory>
#include <string>

#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "storage/dependency_validation/dependency_validation_write_set.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

class DependencyValidationWriteSetTest : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetUp();
    table = std::make_shared<Table>(
        TableColumnDefinitions{{"lhs", DataType::Int, false}, {"rhs", DataType::String, true}}, TableType::Data);
    tree = std::make_shared<DVTree>(DependencyKind::FD);
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<DVTree> tree;
};

TEST_F(DependencyValidationWriteSetTest, GroupsMultiplicityByTableAndDependency) {
  auto write_set = DependencyValidationWriteSet{};
  write_set.insert(table, tree, "FD [0] -> [1]", "lhs-a", "rhs-x");
  write_set.insert(table, tree, "FD [0] -> [1]", "lhs-a", "rhs-x");
  write_set.insert(table, tree, "FD [0] -> [1]", "lhs-b", "rhs-y");

  ASSERT_EQ(write_set.batches().size(), 1u);
  const auto& batch = write_set.batches().front();
  EXPECT_EQ(batch.table, table);
  EXPECT_EQ(batch.tree, tree);
  EXPECT_EQ(batch.dependency_name, "FD [0] -> [1]");
  ASSERT_EQ(batch.transaction.ops.size(), 3u);
  EXPECT_FALSE(batch.transaction.ops[0].is_delete);
  EXPECT_FALSE(batch.transaction.ops[1].is_delete);
  EXPECT_FALSE(batch.transaction.ops[2].is_delete);
}

TEST_F(DependencyValidationWriteSetTest, CancelsInverseOperationsBeforeCidAssignment) {
  auto write_set = DependencyValidationWriteSet{};
  write_set.insert(table, tree, "FD [0] -> [1]", "lhs", "rhs");
  write_set.remove(table, tree, "FD [0] -> [1]", "lhs", "rhs");

  EXPECT_TRUE(write_set.empty());
  EXPECT_TRUE(write_set.batches().empty());
}

TEST_F(DependencyValidationWriteSetTest, UpdateStagesOldRemovalAndNewInsertion) {
  auto write_set = DependencyValidationWriteSet{};
  write_set.update(table, tree, "FD [0] -> [1]", "lhs", "old-rhs", "new-rhs");

  ASSERT_EQ(write_set.batches().size(), 1u);
  const auto& operations = write_set.batches().front().transaction.ops;
  ASSERT_EQ(operations.size(), 2u);
  EXPECT_TRUE(operations[0].is_delete);
  EXPECT_EQ(operations[0].lhs_norm, "lhs");
  EXPECT_EQ(operations[0].rhs_norm, "old-rhs");
  EXPECT_FALSE(operations[1].is_delete);
  EXPECT_EQ(operations[1].lhs_norm, "lhs");
  EXPECT_EQ(operations[1].rhs_norm, "new-rhs");
}

TEST_F(DependencyValidationWriteSetTest, GroupsStoredTableAndGetTableWrapperBySharedTree) {
  auto write_set = DependencyValidationWriteSet{};
  auto get_table_wrapper = std::make_shared<Table>(table->column_definitions(), TableType::Data);

  write_set.remove(get_table_wrapper, tree, "FD [0] -> [1]", "lhs", "old-rhs");
  write_set.insert(table, tree, "FD [0] -> [1]", "lhs", "new-rhs");

  ASSERT_EQ(write_set.batches().size(), 1u);
  EXPECT_EQ(write_set.batches().front().table, get_table_wrapper);
  EXPECT_EQ(write_set.batches().front().tree, tree);
  ASSERT_EQ(write_set.batches().front().transaction.ops.size(), 2u);
}

TEST_F(DependencyValidationWriteSetTest, TransactionContextOwnsAndDiscardsWriteSetOnRollback) {
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  context->stage_dependency_insert(table, tree, "FD [0] -> [1]", "lhs", "rhs");

  ASSERT_NE(context->dependency_validation_write_set(), nullptr);
  ASSERT_EQ(context->dependency_validation_write_set()->batches().size(), 1u);

  context->rollback(RollbackReason::User);
  EXPECT_EQ(context->dependency_validation_write_set(), nullptr);
}

}  // namespace hyrise::dv_tree
