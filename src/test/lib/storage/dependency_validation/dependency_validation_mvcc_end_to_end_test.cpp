#include <memory>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/dependency_validation/dependency_validation_bootstrap.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

using namespace expression_functional;  // NOLINT(build/namespaces)

// Phase 14 MVCC/SI end-to-end tests: drive real Hyrise transactions (Insert /
// Validate+TableScan+Delete operators, TransactionContext::commit) against a
// table with a registered DV-Tree and cross-check every verdict against an
// independent batch recomputation over the rows visible at that snapshot
// (scan_visible_dependency_rows + batch_fd/od_violation_count -- the Phase 12
// oracles, which share no state with the DVTree engine).
class DependencyValidationMvccEndToEndTest : public BaseTest {
 protected:
  static constexpr auto kTableName = "dv_e2e";

  void SetUp() override {
    BaseTest::SetUp();
    _column_definitions =
        TableColumnDefinitions{{"lhs", DataType::Int, false}, {"rhs", DataType::String, false}};
    _table = std::make_shared<Table>(_column_definitions, TableType::Data, std::optional<ChunkOffset>{ChunkOffset{100}},
                                     UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(kTableName, _table);
  }

  // Registers and bootstraps the single validator at the current last commit
  // ID. Called by each test after choosing the dependency kind so FD and OD
  // tests share one fixture.
  void attach_validator(const DependencyKind kind) {
    _spec = DependencySpec{.kind = kind,
                           .lhs_column_ids = {ColumnID{0}},
                           .rhs_column_ids = {ColumnID{1}},
                           .lhs_column_types = {DataType::Int},
                           .rhs_column_types = {DataType::String}};
    _build_cid = Hyrise::get().transaction_manager.last_commit_id();
    _table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, kind, _build_cid);
  }

  DependencyValidator validator() const {
    const auto api = _table->dependency_validation_api();
    Assert(api.size() == 1, "Expected exactly one registered validator.");
    return api.front();
  }

  // Runs one Insert transaction through the full lifecycle and returns its
  // commit ID (the snapshot at which its rows and its DV changes appear).
  CommitID commit_insert(const std::vector<std::pair<int32_t, pmr_string>>& rows) {
    auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
    for (const auto& [lhs, rhs] : rows) {
      values->append({AllTypeVariant{lhs}, AllTypeVariant{rhs}});
    }
    const auto wrapper = std::make_shared<TableWrapper>(values);
    wrapper->execute();
    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    const auto insert = std::make_shared<Insert>(kTableName, wrapper);
    insert->set_transaction_context(context);
    insert->execute();
    Assert(!insert->execute_failed(), "Insert must not fail in this fixture.");
    context->commit();
    return context->commit_id();
  }

  // Deletes every row currently visible with the given LHS value in one
  // transaction and returns its commit ID.
  CommitID commit_delete_where_lhs(const int32_t lhs) {
    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    const auto get_table = std::make_shared<GetTable>(kTableName);
    get_table->execute();
    const auto validate = std::make_shared<Validate>(get_table);
    validate->set_transaction_context(context);
    validate->execute();
    const auto predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::Equals, pqp_column_(ColumnID{0}, DataType::Int, false, "lhs"), value_(AllTypeVariant{lhs}));
    const auto scan = std::make_shared<TableScan>(validate, predicate);
    scan->execute();
    const auto delete_op = std::make_shared<Delete>(scan);
    delete_op->set_transaction_context(context);
    delete_op->execute();
    Assert(!delete_op->execute_failed(), "Delete must not fail in this fixture.");
    context->commit();
    return context->commit_id();
  }

  // Independent recomputation of the expected violation count at `snapshot`,
  // straight from the MVCC row versions.
  int64_t oracle(const CommitID snapshot) const {
    const auto rows = scan_visible_dependency_rows(*_table, _spec, snapshot);
    return _spec.kind == DependencyKind::FD ? batch_fd_violation_count(rows) : batch_od_violation_count(rows);
  }

  // Checks the DV verdict against the oracle for EVERY committed prefix from
  // the bootstrap snapshot up to the current last commit ID.
  void expect_all_prefixes_match() const {
    const auto handle = validator();
    const auto last = Hyrise::get().transaction_manager.last_commit_id();
    for (auto snapshot = _build_cid; snapshot <= last; ++snapshot) {
      SCOPED_TRACE("snapshot " + std::to_string(snapshot));
      const auto expected = oracle(snapshot);
      EXPECT_EQ(handle.violation_count_at(snapshot), expected);
      EXPECT_EQ(handle.holds_at(snapshot), expected == 0);
    }
  }

  // Randomized DML sequence with a fixed, reported seed: a mix of committed
  // multi-row inserts and delete-by-LHS transactions over a small key domain
  // (to force collisions, tombstones, and resurrections). After EVERY commit
  // the whole verdict history is cross-checked against the oracle.
  void run_randomized_dml(const uint32_t seed) {
    SCOPED_TRACE("seed " + std::to_string(seed));
    auto rng = std::mt19937{seed};
    auto lhs_dist = std::uniform_int_distribution<int32_t>{0, 4};
    auto rhs_dist = std::uniform_int_distribution<int>{0, 4};
    auto row_count_dist = std::uniform_int_distribution<int>{1, 3};
    auto op_dist = std::uniform_int_distribution<int>{0, 9};

    for (auto step = 0; step < 25; ++step) {
      SCOPED_TRACE("step " + std::to_string(step));
      if (op_dist(rng) < 7) {
        auto rows = std::vector<std::pair<int32_t, pmr_string>>{};
        const auto row_count = row_count_dist(rng);
        for (auto row = 0; row < row_count; ++row) {
          rows.emplace_back(lhs_dist(rng), pmr_string(1, static_cast<char>('a' + rhs_dist(rng))));
        }
        commit_insert(rows);
      } else {
        commit_delete_where_lhs(lhs_dist(rng));
      }
      expect_all_prefixes_match();
    }
  }

  TableColumnDefinitions _column_definitions;
  std::shared_ptr<Table> _table;
  DependencySpec _spec;
  CommitID _build_cid{INITIAL_COMMIT_ID};
};

TEST_F(DependencyValidationMvccEndToEndTest, FdVerdictMatchesBatchRecomputationAtEveryCommittedPrefix) {
  attach_validator(DependencyKind::FD);
  // Pin the bootstrap snapshot with a long-running transaction so every
  // committed prefix stays retained and queryable for the whole test.
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  commit_insert({{1, "a"}});
  expect_all_prefixes_match();

  // Duplicate row and an unrelated key: still no violation.
  commit_insert({{1, "a"}, {2, "x"}});
  expect_all_prefixes_match();

  // (1 -> "b") joins (1 -> "a"): one FD violation.
  commit_insert({{1, "b"}});
  expect_all_prefixes_match();

  // A second ambiguous key and a widening of the first: three violations total.
  commit_insert({{2, "y"}, {1, "c"}});
  expect_all_prefixes_match();

  // Deleting every lhs=1 row removes that key's two violations.
  commit_delete_where_lhs(1);
  expect_all_prefixes_match();

  commit_delete_where_lhs(2);
  expect_all_prefixes_match();

  pin->commit();
}

TEST_F(DependencyValidationMvccEndToEndTest, OldSnapshotStaysExactWhileNewerViolationsComeAndGo) {
  attach_validator(DependencyKind::FD);
  const auto base_cid = commit_insert({{1, "a"}});

  // A long-running transaction holds snapshot base_cid while newer commits
  // introduce and then remove violations around it.
  const auto old_reader = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  ASSERT_EQ(old_reader->snapshot_commit_id(), base_cid);
  const auto handle = validator();

  const auto violation_cid = commit_insert({{1, "b"}});
  EXPECT_EQ(handle.violation_count_at(violation_cid), 1);
  EXPECT_TRUE(handle.holds_at(base_cid));
  EXPECT_EQ(handle.violation_count_at(base_cid), 0);

  const auto second_violation_cid = commit_insert({{1, "c"}});
  EXPECT_EQ(handle.violation_count_at(second_violation_cid), 2);
  EXPECT_TRUE(handle.holds_at(base_cid));

  const auto resolved_cid = commit_delete_where_lhs(1);
  EXPECT_TRUE(handle.holds_at(resolved_cid));
  // The old snapshot still sees exactly its own row set and verdict.
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, base_cid).size(), 1u);
  EXPECT_TRUE(handle.holds_at(base_cid));
  EXPECT_EQ(handle.violation_count_at(base_cid), oracle(base_cid));

  old_reader->commit();
}

TEST_F(DependencyValidationMvccEndToEndTest, UncommittedChangesAreInvisibleUntilCommitPublishes) {
  attach_validator(DependencyKind::FD);
  const auto base_cid = commit_insert({{1, "a"}});
  const auto handle = validator();

  // Execute a violating insert but do not commit yet: the staged rows exist in
  // the chunk with an active writer TID, so neither row visibility nor the DV
  // verdict at any committed snapshot may change.
  auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
  values->append({AllTypeVariant{int32_t{1}}, AllTypeVariant{pmr_string{"b"}}});
  const auto wrapper = std::make_shared<TableWrapper>(values);
  wrapper->execute();
  const auto writer = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto insert = std::make_shared<Insert>(kTableName, wrapper);
  insert->set_transaction_context(writer);
  insert->execute();
  ASSERT_FALSE(insert->execute_failed());

  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, base_cid).size(), 1u);
  EXPECT_TRUE(handle.holds_at(base_cid));
  EXPECT_EQ(handle.visible_commit_id(), base_cid);

  writer->commit();
  const auto commit_cid = writer->commit_id();

  // Row visibility and the DV verdict switch at the same commit ID: nothing at
  // commit_cid - 1, everything at commit_cid.
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, CommitID{commit_cid - 1}).size(), 1u);
  EXPECT_EQ(handle.violation_count_at(CommitID{commit_cid - 1}), 0);
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, commit_cid).size(), 2u);
  EXPECT_EQ(handle.violation_count_at(commit_cid), 1);
  EXPECT_FALSE(handle.holds_at(commit_cid));
}

TEST_F(DependencyValidationMvccEndToEndTest, RolledBackTransactionLeavesVerdictHistoryUntouched) {
  attach_validator(DependencyKind::FD);
  const auto base_cid = commit_insert({{1, "a"}});
  const auto handle = validator();

  auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
  values->append({AllTypeVariant{int32_t{1}}, AllTypeVariant{pmr_string{"b"}}});
  const auto wrapper = std::make_shared<TableWrapper>(values);
  wrapper->execute();
  const auto writer = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto insert = std::make_shared<Insert>(kTableName, wrapper);
  insert->set_transaction_context(writer);
  insert->execute();
  ASSERT_FALSE(insert->execute_failed());
  writer->rollback(RollbackReason::User);

  // No commit ID was consumed by the DV path and the verdict is unchanged; a
  // subsequent commit continues seamlessly from the same history.
  EXPECT_EQ(handle.visible_commit_id(), base_cid);
  EXPECT_TRUE(handle.holds_at(base_cid));

  const auto next_cid = commit_insert({{2, "x"}});
  EXPECT_TRUE(handle.holds_at(next_cid));
  EXPECT_EQ(handle.violation_count_at(next_cid), oracle(next_cid));
}

TEST_F(DependencyValidationMvccEndToEndTest, OdVerdictMatchesBatchRecomputationAtEveryCommittedPrefix) {
  attach_validator(DependencyKind::OD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // Ordered baseline: lhs order 1 < 2 < 3 matches rhs order "a" < "m" < "z".
  commit_insert({{1, "a"}, {2, "m"}, {3, "z"}});
  expect_all_prefixes_match();

  // Local violation: lhs 1 now maps to two distinct rhs values.
  commit_insert({{1, "b"}});
  expect_all_prefixes_match();

  // Neighbor violation: (2, "zz") makes group 2's maximum rhs sort after
  // group 3's minimum "z".
  commit_insert({{2, "zz"}});
  expect_all_prefixes_match();

  // Removing all lhs=2 rows removes the neighbor violation; groups 1 and 3
  // become adjacent and stay ordered.
  commit_delete_where_lhs(2);
  expect_all_prefixes_match();

  commit_delete_where_lhs(1);
  expect_all_prefixes_match();

  pin->commit();
}

TEST_F(DependencyValidationMvccEndToEndTest, MultipleOperatorsAndSelfCancellationInOneTransaction) {
  attach_validator(DependencyKind::FD);
  commit_insert({{1, "a"}});

  // One transaction, three operators: two Insert operators and a Delete that
  // removes a row the SAME transaction just inserted. The staged insert and
  // remove of (3, "x") cancel in the write set, so the net dependency change
  // of the whole transaction is exactly +(1, "b").
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  for (const auto& [lhs, rhs] : std::vector<std::pair<int32_t, pmr_string>>{{3, "x"}, {1, "b"}}) {
    auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
    values->append({AllTypeVariant{lhs}, AllTypeVariant{rhs}});
    const auto wrapper = std::make_shared<TableWrapper>(values);
    wrapper->execute();
    const auto insert = std::make_shared<Insert>(kTableName, wrapper);
    insert->set_transaction_context(context);
    insert->execute();
    ASSERT_FALSE(insert->execute_failed());
  }

  const auto get_table = std::make_shared<GetTable>(kTableName);
  get_table->execute();
  const auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(context);
  validate->execute();
  const auto predicate = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals, pqp_column_(ColumnID{0}, DataType::Int, false, "lhs"), value_(AllTypeVariant{3}));
  const auto scan = std::make_shared<TableScan>(validate, predicate);
  scan->execute();
  const auto delete_op = std::make_shared<Delete>(scan);
  delete_op->set_transaction_context(context);
  delete_op->execute();
  ASSERT_FALSE(delete_op->execute_failed());

  context->commit();
  const auto commit_cid = context->commit_id();

  // Only (1, "a") and (1, "b") are visible; (3, "x") never became visible.
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, commit_cid).size(), 2u);
  EXPECT_EQ(validator().violation_count_at(commit_cid), 1);
  expect_all_prefixes_match();
}

TEST_F(DependencyValidationMvccEndToEndTest, UpdateOperatorOldNewPairMatchesOracle) {
  attach_validator(DependencyKind::FD);
  commit_insert({{1, "a"}});
  const auto pre_update_cid = commit_insert({{2, "b"}});
  EXPECT_TRUE(validator().holds_at(pre_update_cid));

  // Real Update operator (internally a Delete+Insert pair in one transaction):
  // move row (2, "b") to LHS 1, so the old pair is removed and the new pair
  // (1, "b") collides with (1, "a").
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto get_table = std::make_shared<GetTable>(kTableName);
  get_table->execute();
  const auto validate = std::make_shared<Validate>(get_table);
  validate->set_transaction_context(context);
  validate->execute();
  const auto predicate = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals, pqp_column_(ColumnID{0}, DataType::Int, false, "lhs"), value_(AllTypeVariant{2}));
  const auto where_scan = std::make_shared<TableScan>(validate, predicate);
  where_scan->never_clear_output();
  where_scan->execute();
  const auto updated_values = std::make_shared<Projection>(
      where_scan, expression_vector(value_(AllTypeVariant{1}), pqp_column_(ColumnID{1}, DataType::String, false, "rhs")));
  updated_values->execute();
  const auto update = std::make_shared<Update>(kTableName, where_scan, updated_values);
  update->set_transaction_context(context);
  update->execute();
  ASSERT_FALSE(update->execute_failed());
  context->commit();
  const auto update_cid = context->commit_id();

  EXPECT_EQ(validator().violation_count_at(CommitID{update_cid - 1}), 0);
  EXPECT_EQ(validator().violation_count_at(update_cid), 1);
  expect_all_prefixes_match();
}

TEST_F(DependencyValidationMvccEndToEndTest, ConflictRollbackLeavesVerdictUnchanged) {
  attach_validator(DependencyKind::FD);
  const auto violation_cid = commit_insert({{1, "a"}, {1, "b"}});
  EXPECT_EQ(validator().violation_count_at(violation_cid), 1);

  // Two transactions race to delete the same rows. The first locks them; the
  // second's Delete fails and the transaction rolls back with a conflict.
  const auto winner = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto winner_get_table = std::make_shared<GetTable>(kTableName);
  winner_get_table->execute();
  const auto winner_validate = std::make_shared<Validate>(winner_get_table);
  winner_validate->set_transaction_context(winner);
  winner_validate->execute();
  const auto winner_delete = std::make_shared<Delete>(winner_validate);
  winner_delete->set_transaction_context(winner);
  winner_delete->execute();
  ASSERT_FALSE(winner_delete->execute_failed());

  const auto loser = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto loser_get_table = std::make_shared<GetTable>(kTableName);
  loser_get_table->execute();
  const auto loser_validate = std::make_shared<Validate>(loser_get_table);
  loser_validate->set_transaction_context(loser);
  loser_validate->execute();
  const auto loser_delete = std::make_shared<Delete>(loser_validate);
  loser_delete->set_transaction_context(loser);
  loser_delete->execute();
  ASSERT_TRUE(loser_delete->execute_failed());
  loser->rollback(RollbackReason::Conflict);

  winner->commit();
  const auto resolved_cid = winner->commit_id();

  // Exactly one removal took effect; the loser contributed nothing and the
  // verdict matches the oracle at every prefix (no stranded CID either: a
  // later commit proceeds normally).
  EXPECT_TRUE(validator().holds_at(resolved_cid));
  commit_insert({{5, "q"}});
  expect_all_prefixes_match();
}

TEST_F(DependencyValidationMvccEndToEndTest, OneTransactionTouchesMultipleTablesAndDependencies) {
  attach_validator(DependencyKind::FD);
  commit_insert({{1, "a"}});

  // A second table with its own FD validator and a third table with no
  // validator at all (the dependency-unrelated write).
  const auto other_table = std::make_shared<Table>(_column_definitions, TableType::Data,
                                                   std::optional<ChunkOffset>{ChunkOffset{100}}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("dv_e2e_other", other_table);
  other_table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, DependencyKind::FD,
                                                     Hyrise::get().transaction_manager.last_commit_id());
  const auto plain_table = std::make_shared<Table>(_column_definitions, TableType::Data,
                                                   std::optional<ChunkOffset>{ChunkOffset{100}}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("dv_e2e_plain", plain_table);

  // One transaction inserts into all three tables: a violating pair into the
  // fixture table, a clean row into the second, and a row into the plain one.
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  for (const auto& [target, lhs, rhs] :
       std::vector<std::tuple<std::string, int32_t, pmr_string>>{{kTableName, 1, "b"}, {"dv_e2e_other", 7, "z"},
                                                                 {"dv_e2e_plain", 9, "p"}}) {
    auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
    values->append({AllTypeVariant{lhs}, AllTypeVariant{rhs}});
    const auto wrapper = std::make_shared<TableWrapper>(values);
    wrapper->execute();
    const auto insert = std::make_shared<Insert>(target, wrapper);
    insert->set_transaction_context(context);
    insert->execute();
    ASSERT_FALSE(insert->execute_failed());
  }
  context->commit();
  const auto commit_cid = context->commit_id();

  // Both validators advance atomically at the SAME commit ID and each matches
  // its own oracle; the plain table simply received its row.
  EXPECT_EQ(validator().violation_count_at(commit_cid), 1);
  expect_all_prefixes_match();

  const auto other_api = other_table->dependency_validation_api();
  ASSERT_EQ(other_api.size(), 1u);
  EXPECT_TRUE(other_api.front().holds_at(commit_cid));
  const auto other_rows = scan_visible_dependency_rows(*other_table, _spec, commit_cid);
  EXPECT_EQ(other_rows.size(), 1u);
  EXPECT_EQ(other_api.front().violation_count_at(commit_cid), batch_fd_violation_count(other_rows));
  EXPECT_EQ(plain_table->row_count(), 1u);
}

TEST_F(DependencyValidationMvccEndToEndTest, TombstoneRunAndResurrectionThroughOperators) {
  attach_validator(DependencyKind::FD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // Create, delete, and resurrect the SAME normalized keys twice: the tree
  // must run through tombstones and back at every step.
  commit_insert({{1, "a"}, {1, "b"}});
  expect_all_prefixes_match();
  commit_delete_where_lhs(1);
  expect_all_prefixes_match();
  commit_insert({{1, "a"}, {1, "b"}});
  expect_all_prefixes_match();
  commit_delete_where_lhs(1);
  expect_all_prefixes_match();

  const auto final_cid = Hyrise::get().transaction_manager.last_commit_id();
  EXPECT_TRUE(validator().holds_at(final_cid));
  pin->commit();
}

TEST_F(DependencyValidationMvccEndToEndTest, RandomizedFdDmlMatchesOracleAtEveryCommittedPrefix) {
  attach_validator(DependencyKind::FD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  run_randomized_dml(/*seed=*/0xD514u);
  pin->commit();
}

TEST_F(DependencyValidationMvccEndToEndTest, RandomizedOdDmlMatchesOracleAtEveryCommittedPrefix) {
  attach_validator(DependencyKind::OD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  run_randomized_dml(/*seed=*/0x0D14u);
  pin->commit();
}

// Composite-key variant: two LHS columns and one RHS column. Exercises the
// same end-to-end path with self-delimiting multi-column normalized keys.
class DependencyValidationMvccCompositeKeyTest : public BaseTest {
 protected:
  static constexpr auto kTableName = "dv_e2e_composite";

  void SetUp() override {
    BaseTest::SetUp();
    _column_definitions = TableColumnDefinitions{
        {"l1", DataType::Int, false}, {"l2", DataType::String, false}, {"rhs", DataType::Int, false}};
    _table = std::make_shared<Table>(_column_definitions, TableType::Data, std::optional<ChunkOffset>{ChunkOffset{100}},
                                     UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(kTableName, _table);

  }

  void attach_validator(const DependencyKind kind) {
    _spec = DependencySpec{.kind = kind,
                           .lhs_column_ids = {ColumnID{0}, ColumnID{1}},
                           .rhs_column_ids = {ColumnID{2}},
                           .lhs_column_types = {DataType::Int, DataType::String},
                           .rhs_column_types = {DataType::Int}};
    _build_cid = Hyrise::get().transaction_manager.last_commit_id();
    _table->build_and_attach_dependency_validator({ColumnID{0}, ColumnID{1}}, {ColumnID{2}}, kind, _build_cid);
  }

  CommitID commit_insert(const std::vector<std::tuple<int32_t, pmr_string, int32_t>>& rows) {
    auto values = std::make_shared<Table>(_column_definitions, TableType::Data);
    for (const auto& [l1, l2, rhs] : rows) {
      values->append({AllTypeVariant{l1}, AllTypeVariant{l2}, AllTypeVariant{rhs}});
    }
    const auto wrapper = std::make_shared<TableWrapper>(values);
    wrapper->execute();
    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    const auto insert = std::make_shared<Insert>(kTableName, wrapper);
    insert->set_transaction_context(context);
    insert->execute();
    Assert(!insert->execute_failed(), "Insert must not fail in this fixture.");
    context->commit();
    return context->commit_id();
  }

  void expect_all_prefixes_match() const {
    const auto api = _table->dependency_validation_api();
    ASSERT_EQ(api.size(), 1u);
    const auto& handle = api.front();
    const auto last = Hyrise::get().transaction_manager.last_commit_id();
    for (auto snapshot = _build_cid; snapshot <= last; ++snapshot) {
      SCOPED_TRACE("snapshot " + std::to_string(snapshot));
      const auto rows = scan_visible_dependency_rows(*_table, _spec, snapshot);
      const auto expected =
          _spec.kind == DependencyKind::FD ? batch_fd_violation_count(rows) : batch_od_violation_count(rows);
      EXPECT_EQ(handle.violation_count_at(snapshot), expected);
      EXPECT_EQ(handle.holds_at(snapshot), expected == 0);
    }
  }

  TableColumnDefinitions _column_definitions;
  std::shared_ptr<Table> _table;
  DependencySpec _spec;
  CommitID _build_cid{INITIAL_COMMIT_ID};
};

TEST_F(DependencyValidationMvccCompositeKeyTest, CompositeFdVerdictMatchesBatchRecomputation) {
  attach_validator(DependencyKind::FD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // (1, "a") and (1, "b") are DIFFERENT composite LHS keys: same rhs or
  // different rhs, no violation between them.
  commit_insert({{1, "a", 10}, {1, "b", 20}});
  expect_all_prefixes_match();

  // The same composite key (1, "a") maps to a second rhs: one violation.
  commit_insert({{1, "a", 30}});
  expect_all_prefixes_match();

  // A prefix-ambiguous pair: ("1a" vs "1a") must not be confused with
  // (l1=11, l2="") or similar concatenation artifacts.
  commit_insert({{11, "", 40}, {11, "", 40}});
  expect_all_prefixes_match();

  pin->commit();
}

TEST_F(DependencyValidationMvccCompositeKeyTest, CompositeOdVerdictMatchesBatchRecomputation) {
  attach_validator(DependencyKind::OD);
  const auto pin = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  // Composite LHS keys in normalized order: (1,"a") < (1,"b") < (2,"a").
  // Their rhs values 10 < 20 < 30 are ordered, so the OD holds.
  commit_insert({{1, "a", 10}, {1, "b", 20}, {2, "a", 30}});
  expect_all_prefixes_match();

  // Local violation on the composite key (1, "b").
  commit_insert({{1, "b", 25}});
  expect_all_prefixes_match();

  // Neighbor violation: (1, "b")'s maximum rhs 35 now exceeds (2, "a")'s
  // minimum rhs 30.
  commit_insert({{1, "b", 35}});
  expect_all_prefixes_match();

  pin->commit();
}

}  // namespace hyrise::dv_tree
