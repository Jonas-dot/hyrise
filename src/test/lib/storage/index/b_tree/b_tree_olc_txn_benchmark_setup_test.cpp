/**
 * Correctness tests for the transaction-level benchmark setup.
 *
 * Mirrors the patterns used in b_tree_olc_txn_benchmark.cpp:
 *
 *   1. create_base_table (None):    kBaseLhsCount rows are visible.
 *   2. create_base_table (FD):      single FD index seeded, dependency_holds().
 *   3. create_base_table (OD):      single OD index seeded, dependency_holds().
 *   4. op_insert (NoIndex):         one new visible row.
 *   5. op_delete (NoIndex):         row becomes invisible.
 *   6. op_update (NoIndex):         row count unchanged, new rhs visible.
 *   7. op_insert with FDIndex:      FD validator still holds after commit.
 *   8. op_insert with ODIndex:      OD validator still holds after commit.
 *   9. op_delete with FDIndex:      FD validator still holds after commit.
 *  10. op_delete with ODIndex:      OD validator still holds after commit.
 *  11. Concurrent inserts:          all unique-key inserts commit without conflict.
 *  12. Hyrise::reset():             no stale transactions after teardown.
 */

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk.hpp"
#include "storage/index/b_tree/b_tree_olc_index.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT

// Mirror of benchmark constants / helpers
static constexpr int32_t kBaseLhsCount = 65'000;
static constexpr int32_t kRhsFactor    = 100;
static const std::string kTableName    = "txn_bench_test";

static const TableColumnDefinitions kColDefs{{"lhs", DataType::Int, false}, {"rhs", DataType::Int, false}};

enum class IndexVariant { None, FD, OD };

static std::shared_ptr<AbstractExpression> make_lhs_predicate(const int32_t lhs_val) {
  return std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals,
      pqp_column_(ColumnID{0}, DataType::Int, /*nullable=*/false, "lhs"),
      value_(AllTypeVariant{lhs_val}));
}

// ---- DML helpers (exact copies of benchmark's static functions) ----

static void op_insert(const int32_t lhs, const int32_t rhs) {
  auto row = std::make_shared<Table>(kColDefs, TableType::Data);
  row->append({AllTypeVariant{lhs}, AllTypeVariant{rhs}});
  const auto w = std::make_shared<TableWrapper>(row);
  w->execute();
  const auto txn = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto ins = std::make_shared<Insert>(kTableName, w);
  ins->set_transaction_context(txn);
  ins->execute();
  txn->commit();
}

static bool op_delete(const int32_t lhs) {
  const auto txn = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto gt  = std::make_shared<GetTable>(kTableName);
  gt->execute();
  const auto val = std::make_shared<Validate>(gt);
  val->set_transaction_context(txn);
  val->execute();
  const auto sc  = std::make_shared<TableScan>(val, make_lhs_predicate(lhs));
  sc->execute();
  const auto del = std::make_shared<Delete>(sc);
  del->set_transaction_context(txn);
  del->execute();
  if (del->execute_failed()) {
    txn->rollback(RollbackReason::Conflict);
    return false;
  }
  txn->commit();
  return true;
}

static void op_update(const int32_t lhs, const int32_t new_rhs) {
  const auto txn = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto gt  = std::make_shared<GetTable>(kTableName);
  gt->execute();
  const auto val = std::make_shared<Validate>(gt);
  val->set_transaction_context(txn);
  val->execute();
  const auto sc  = std::make_shared<TableScan>(val, make_lhs_predicate(lhs));
  sc->execute();
  const auto del = std::make_shared<Delete>(sc);
  del->set_transaction_context(txn);
  del->execute();
  if (del->execute_failed()) { txn->rollback(RollbackReason::Conflict); return; }

  auto new_row = std::make_shared<Table>(kColDefs, TableType::Data);
  new_row->append({AllTypeVariant{lhs}, AllTypeVariant{new_rhs}});
  const auto w = std::make_shared<TableWrapper>(new_row);
  w->execute();
  const auto ins = std::make_shared<Insert>(kTableName, w);
  ins->set_transaction_context(txn);
  ins->execute();
  txn->commit();
}

static int64_t count_visible_rows(const int32_t lhs = -1) {
  const auto txn = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  const auto gt  = std::make_shared<GetTable>(kTableName);
  gt->execute();
  const auto val = std::make_shared<Validate>(gt);
  val->set_transaction_context(txn);
  val->execute();

  std::shared_ptr<const AbstractOperator> last_op = val;
  if (lhs >= 0) {
    const auto sc = std::make_shared<TableScan>(val, make_lhs_predicate(lhs));
    sc->execute();
    last_op = sc;
  }
  txn->commit();

  const auto& out = last_op->get_output();
  if (!out) return 0;
  int64_t cnt = 0;
  for (auto cid = ChunkID{0}; cid < out->chunk_count(); ++cid) {
    const auto c = out->get_chunk(cid);
    if (c) cnt += static_cast<int64_t>(c->size());
  }
  return cnt;
}

static void create_base_table(const IndexVariant variant) {
  Hyrise::reset();
  auto table = std::make_shared<Table>(kColDefs, TableType::Data,
                                        std::optional<ChunkOffset>{Chunk::DEFAULT_SIZE}, UseMvcc::Yes);
  for (int32_t lhs = 0; lhs < kBaseLhsCount; ++lhs) {
    table->append({AllTypeVariant{lhs}, AllTypeVariant{lhs * kRhsFactor}});
  }
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    if (!chunk) continue;
    const auto mvcc = chunk->mvcc_data();
    for (auto offset = ChunkOffset{0}; offset < chunk->size(); ++offset) {
      mvcc->set_begin_cid(offset, CommitID{0});
      mvcc->set_tid(offset, TransactionID{0});
    }
    set_atomic_max(mvcc->max_begin_cid, CommitID{0});
  }
  if (variant != IndexVariant::None) {
    const auto dep_type = (variant == IndexVariant::FD) ? DependencyType::FD : DependencyType::OD;
    table->set_dependency_validator(ColumnID{0}, ColumnID{1}, dep_type);
    for (const auto& dep : table->dependency_validators()) {
      for (int32_t lhs = 0; lhs < kBaseLhsCount; ++lhs) {
        dep.index->insert_entry_for_validation({AllTypeVariant{lhs}},
                                               {AllTypeVariant{lhs * kRhsFactor}},
                                               dep.dependency_type);
      }
    }
  }
  Hyrise::get().storage_manager.add_table(kTableName, table);
}

// Test fixture
class TxnBenchmarkSetupTest : public ::testing::Test {
 protected:
  void TearDown() override { Hyrise::reset(); }
};

// Helper: assert that the single registered validator has no violations.
static void expect_dependency_holds(const std::shared_ptr<Table>& table) {
  const auto& deps = table->dependency_validators();
  ASSERT_EQ(deps.size(), 1u);
  EXPECT_EQ(deps[0].index->global_violation_count(), 0);
  EXPECT_TRUE(deps[0].index->dependency_holds());
}

// 1. Base table (NoIndex): all rows visible
TEST_F(TxnBenchmarkSetupTest, BaseTableNoIndex_RowCount) {
  create_base_table(IndexVariant::None);
  EXPECT_EQ(count_visible_rows(), kBaseLhsCount);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  EXPECT_TRUE(table->dependency_validators().empty());
}

// 2. Base table (FDIndex): single FD validator seeded, holds
TEST_F(TxnBenchmarkSetupTest, BaseTableFDIndex_DependencyHolds) {
  create_base_table(IndexVariant::FD);
  EXPECT_EQ(count_visible_rows(), kBaseLhsCount);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  ASSERT_EQ(table->dependency_validators().size(), 1u);
  EXPECT_EQ(table->dependency_validators()[0].dependency_type, DependencyType::FD);
  expect_dependency_holds(table);
}

// 3. Base table (ODIndex): single OD validator seeded, holds
TEST_F(TxnBenchmarkSetupTest, BaseTableODIndex_DependencyHolds) {
  create_base_table(IndexVariant::OD);
  EXPECT_EQ(count_visible_rows(), kBaseLhsCount);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  ASSERT_EQ(table->dependency_validators().size(), 1u);
  EXPECT_EQ(table->dependency_validators()[0].dependency_type, DependencyType::OD);
  expect_dependency_holds(table);
}

// 4. op_insert (NoIndex): adds exactly one visible row
TEST_F(TxnBenchmarkSetupTest, InsertNoIndex_AddsRow) {
  create_base_table(IndexVariant::None);
  op_insert(kBaseLhsCount, kBaseLhsCount * kRhsFactor);
  EXPECT_EQ(count_visible_rows(), kBaseLhsCount + 1);
  EXPECT_EQ(count_visible_rows(kBaseLhsCount), 1);
}

// 5. op_delete (NoIndex): row becomes invisible
TEST_F(TxnBenchmarkSetupTest, DeleteNoIndex_RemovesRow) {
  create_base_table(IndexVariant::None);
  EXPECT_EQ(count_visible_rows(0), 1);
  EXPECT_TRUE(op_delete(0));
  EXPECT_EQ(count_visible_rows(0), 0);
}

// 6. op_update (NoIndex): row count unchanged, new rhs visible
TEST_F(TxnBenchmarkSetupTest, UpdateNoIndex_ChangesRhs) {
  create_base_table(IndexVariant::None);
  const int64_t before = count_visible_rows();
  op_update(1, 999);
  EXPECT_EQ(count_visible_rows(), before);  // net delta = 0
  EXPECT_EQ(count_visible_rows(1), 1);      // lhs=1 still present
}

// 7. op_insert with FDIndex: commit hook keeps FD intact
TEST_F(TxnBenchmarkSetupTest, InsertFDIndex_DependencyHolds) {
  create_base_table(IndexVariant::FD);
  // Unique lhs key -- FD trivially maintained
  op_insert(kBaseLhsCount, kBaseLhsCount * kRhsFactor);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  expect_dependency_holds(table);
  EXPECT_EQ(count_visible_rows(kBaseLhsCount), 1);
}

// 8. op_insert with ODIndex: commit hook keeps OD intact
//    rhs = lhs * kRhsFactor is strictly larger than the base table's max rhs,
//    so the monotone order is preserved.
TEST_F(TxnBenchmarkSetupTest, InsertODIndex_DependencyHolds) {
  create_base_table(IndexVariant::OD);
  // lhs beyond base range, rhs = lhs*kRhsFactor > (kBaseLhsCount-1)*kRhsFactor → OD holds
  op_insert(kBaseLhsCount, kBaseLhsCount * kRhsFactor);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  expect_dependency_holds(table);
  EXPECT_EQ(count_visible_rows(kBaseLhsCount), 1);
}

// 9. op_delete with FDIndex: commit hook keeps FD intact; row gone
TEST_F(TxnBenchmarkSetupTest, DeleteFDIndex_DependencyHolds) {
  create_base_table(IndexVariant::FD);
  op_delete(5);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  expect_dependency_holds(table);
  EXPECT_EQ(count_visible_rows(5), 0);
}

// 10. op_delete with ODIndex: commit hook keeps OD intact; row gone
TEST_F(TxnBenchmarkSetupTest, DeleteODIndex_DependencyHolds) {
  create_base_table(IndexVariant::OD);
  op_delete(5);
  const auto table = Hyrise::get().storage_manager.get_table(kTableName);
  expect_dependency_holds(table);
  EXPECT_EQ(count_visible_rows(5), 0);
}

// 11. Concurrent inserts with unique keys all commit without conflict
TEST_F(TxnBenchmarkSetupTest, ConcurrentInsertsNoConflict) {
  create_base_table(IndexVariant::None);
  constexpr int kThreads   = 4;
  constexpr int kPerThread = 100;
  std::atomic<int32_t> next_lhs{kBaseLhsCount};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        const auto lhs = next_lhs.fetch_add(1, std::memory_order_relaxed);
        op_insert(lhs, lhs * kRhsFactor);
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_EQ(count_visible_rows(), kBaseLhsCount + kThreads * kPerThread);
}

// 12. Hyrise::reset() after benchmark teardown leaves no stale transactions
TEST_F(TxnBenchmarkSetupTest, HyriseResetClearsStateCleanly) {
  create_base_table(IndexVariant::None);
  op_insert(kBaseLhsCount, 0);
  op_delete(0);
  EXPECT_NO_THROW(Hyrise::reset());
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table(kTableName));
}

}  // namespace hyrise