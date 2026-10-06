#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dependency_validation/dependency_validator.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

template <typename T>
concept HasPublicCommitEntryPoint = requires(T& tree) { tree.begin_commit(CommitID{2}); };

template <typename T>
concept HasMutableDependencyDescriptorAccessor = requires(const T& table) { table.dependency_validators(); };

static_assert(!HasPublicCommitEntryPoint<DVTree>);
static_assert(!HasMutableDependencyDescriptorAccessor<Table>);

class DependencyValidatorTest : public BaseTest {
 protected:
  static constexpr auto kTableName = "dependency_validator_test";

  void SetUp() override {
    BaseTest::SetUp();
    table = std::make_shared<Table>(
        TableColumnDefinitions{{"lhs", DataType::String, false}, {"rhs", DataType::String, false}}, TableType::Data,
        std::optional<ChunkOffset>{ChunkOffset{10}}, UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(kTableName, table);
    table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, DependencyKind::FD,
                                                 Hyrise::get().transaction_manager.last_commit_id());
  }

  CommitID commit_insert(const std::string& lhs, const std::string& rhs) {
    auto values = std::make_shared<Table>(
        TableColumnDefinitions{{"lhs", DataType::String, false}, {"rhs", DataType::String, false}}, TableType::Data);
    values->append({pmr_string{lhs}, pmr_string{rhs}});
    const auto wrapper = std::make_shared<TableWrapper>(values);
    wrapper->execute();

    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    const auto insert = std::make_shared<Insert>(kTableName, wrapper);
    insert->set_transaction_context(context);
    insert->execute();
    Assert(!insert->execute_failed(), "Insert must succeed in this fixture.");
    context->commit();
    return context->commit_id();
  }

  std::shared_ptr<Table> table;
};

TEST_F(DependencyValidatorTest, ExposesDescriptorMetadata) {
  const auto api = table->dependency_validation_api();
  ASSERT_EQ(api.size(), 1u);
  const auto& validator = api.front();

  EXPECT_EQ(validator.kind(), DependencyKind::FD);
  EXPECT_EQ(validator.lhs_columns(), std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(validator.rhs_columns(), std::vector<ColumnID>{ColumnID{1}});
  EXPECT_EQ(validator.name(), "FD [0] -> [1]");
}

TEST_F(DependencyValidatorTest, ReportsHoldsAndViolationsPerSnapshot) {
  const auto first = commit_insert("k", "a");   // k -> a: the FD still holds
  const auto second = commit_insert("k", "b");  // k -> a and k -> b: the FD is violated

  const auto api = table->dependency_validation_api();
  ASSERT_EQ(api.size(), 1u);
  const auto& validator = api.front();

  EXPECT_TRUE(validator.holds_at(first));
  EXPECT_EQ(validator.violation_count_at(first), 0);

  EXPECT_FALSE(validator.holds_at(second));
  EXPECT_EQ(validator.violation_count_at(second), 1);
}

TEST_F(DependencyValidatorTest, RejectsSnapshotNewerThanVisible) {
  const auto latest = commit_insert("k", "a");
  const auto validator = table->dependency_validation_api().front();

  // A snapshot newer than anything installed/visible cannot be answered: the
  // query throws instead of leaking effects the snapshot must not see.
  const auto too_new = CommitID{static_cast<CommitID::base_type>(latest) + 1};
  EXPECT_THROW(static_cast<void>(validator.holds_at(too_new)), SnapshotNotVisible);
}

TEST_F(DependencyValidatorTest, ReportsSnapshotBoundsAndMemoryStatistics) {
  commit_insert("k", "a");
  const auto validator = table->dependency_validation_api().front();

  EXPECT_GE(validator.visible_commit_id(), CommitID{1});
  EXPECT_NO_THROW(static_cast<void>(validator.oldest_exact_snapshot()));
  EXPECT_NO_THROW(static_cast<void>(validator.memory_statistics()));
}

}  // namespace hyrise::dv_tree
