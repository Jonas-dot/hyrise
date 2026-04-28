// Naive DVI transaction benchmark -- no pooling, no compaction.
// Dead MVCC rows accumulate with each DELETE/UPDATE cycle, demonstrating the
// O(N²) table-scan degradation.
//
// Schema: row_id (unique per row) + lhs + rhs
//   row_id  -- unique identifier used for targeted DELETE/UPDATE
//   lhs     -- determinant column (can repeat when duplicate_rate > 0)
//   rhs     -- dependent column
//
// --duplicate-rate D: fraction [0,1] controlling lhs key collisions.
//   D=0.0 → every row has a unique lhs (no duplicates, pure FD/OD baseline)
//   D=0.5 → each lhs appears ~2 times on average (refcount=2 in DVI)
//   D=0.9 → each lhs appears ~10 times on average
//   num_unique_lhs = max(1, ceil(size * (1 - D)))
//
// Mechanisms: off | fd_single | fd_multi | od_single | od_multi
// Workloads:  insert_only | delete_only | update_only | mixed

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

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
#include "storage/mvcc_data.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

// ---- table schema ----------------------------------------------------------
// Single-col: [row_id, lhs, rhs]       DVI: col1 -> col2
// Multi-col:  [row_id, lhs1, lhs2, rhs1, rhs2]   DVI: {col1,col2} -> {col3,col4}

static const std::string TABLE_SINGLE = "dvi_naive";
static const std::string TABLE_MULTI  = "dvi_naive_multi";

static const hyrise::TableColumnDefinitions COL_SINGLE{
    {"row_id", hyrise::DataType::Int, false},
    {"lhs",    hyrise::DataType::Int, false},
    {"rhs",    hyrise::DataType::Int, false}};

static const hyrise::TableColumnDefinitions COL_MULTI{
    {"row_id", hyrise::DataType::Int, false},
    {"lhs1",   hyrise::DataType::Int, false},
    {"lhs2",   hyrise::DataType::Int, false},
    {"rhs1",   hyrise::DataType::Int, false},
    {"rhs2",   hyrise::DataType::Int, false}};

// Global counter for fresh row_ids (reset to pre_pop at setup)
static std::atomic<int32_t> g_next_row_id{0};

// ---- value helpers ---------------------------------------------------------
static int32_t rhs_for(int32_t lhs)                  { return lhs * 2; }
static int32_t lhs2_for(int32_t lhs1)                { return lhs1 / 1024; }
static int32_t rhs1_for(int32_t lhs1)                { return lhs1 * 2; }
static int32_t rhs2_for(int32_t lhs1, int32_t lhs2)  { return rhs1_for(lhs1) + lhs2; }
static int32_t violation_rhs_for(int32_t lhs)        { return rhs_for(lhs) + 1; }

static uint64_t splitmix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

// ---- mechanism helpers -----------------------------------------------------

enum class Mech { OFF, FD_SINGLE, FD_MULTI, OD_SINGLE, OD_MULTI };

static Mech parse_mech(const std::string& s) {
  if (s == "off")       return Mech::OFF;
  if (s == "fd_single") return Mech::FD_SINGLE;
  if (s == "fd_multi")  return Mech::FD_MULTI;
  if (s == "od_single") return Mech::OD_SINGLE;
  if (s == "od_multi")  return Mech::OD_MULTI;
  throw std::runtime_error("Unknown mechanism: " + s);
}

static bool is_multi(Mech m) {
  return m == Mech::FD_MULTI || m == Mech::OD_MULTI;
}

static const std::string& tname(Mech m) {
  return is_multi(m) ? TABLE_MULTI : TABLE_SINGLE;
}

static hyrise::DependencyType dep_type(Mech m) {
  return (m == Mech::OD_SINGLE || m == Mech::OD_MULTI)
         ? hyrise::DependencyType::OD : hyrise::DependencyType::FD;
}

// Predicate on row_id (ColumnID 0) -- uniquely identifies one row
static std::shared_ptr<hyrise::AbstractExpression> eq_rowid(int32_t val) {
  using namespace hyrise::expression_functional;
  return std::make_shared<hyrise::BinaryPredicateExpression>(
      hyrise::PredicateCondition::Equals,
      pqp_column_(hyrise::ColumnID{0}, hyrise::DataType::Int, false, "row_id"),
      value_(hyrise::AllTypeVariant{val}));
}

// ---- DML primitives --------------------------------------------------------

static void do_insert(Mech m, int32_t row_id, int32_t lhs, int32_t rhs) {
  auto row = std::make_shared<hyrise::Table>(COL_SINGLE, hyrise::TableType::Data);
  row->append({hyrise::AllTypeVariant{row_id},
               hyrise::AllTypeVariant{lhs},
               hyrise::AllTypeVariant{rhs}});
  auto wrapper = std::make_shared<hyrise::TableWrapper>(row);
  auto ins = std::make_shared<hyrise::Insert>(TABLE_SINGLE, wrapper);

  auto txn = hyrise::Hyrise::get().transaction_manager.new_transaction_context(
      hyrise::AutoCommit::No);
  ins->set_transaction_context_recursively(txn);
  wrapper->execute();
  ins->execute();
  txn->commit();
}

static void do_insert_multi(int32_t row_id, int32_t lhs1, int32_t lhs2,
                             int32_t r1, int32_t r2) {
  auto row = std::make_shared<hyrise::Table>(COL_MULTI, hyrise::TableType::Data);
  row->append({hyrise::AllTypeVariant{row_id}, hyrise::AllTypeVariant{lhs1},
               hyrise::AllTypeVariant{lhs2},   hyrise::AllTypeVariant{r1},
               hyrise::AllTypeVariant{r2}});
  auto wrapper = std::make_shared<hyrise::TableWrapper>(row);
  auto ins = std::make_shared<hyrise::Insert>(TABLE_MULTI, wrapper);

  auto txn = hyrise::Hyrise::get().transaction_manager.new_transaction_context(
      hyrise::AutoCommit::No);
  ins->set_transaction_context_recursively(txn);
  wrapper->execute();
  ins->execute();
  txn->commit();
}

// Delete the row with the given row_id. Returns false on MVCC conflict.
static bool do_delete(Mech m, int32_t row_id) {
  auto gt   = std::make_shared<hyrise::GetTable>(tname(m));
  auto val  = std::make_shared<hyrise::Validate>(gt);
  auto scan = std::make_shared<hyrise::TableScan>(val, eq_rowid(row_id));
  auto del  = std::make_shared<hyrise::Delete>(scan);

  auto txn = hyrise::Hyrise::get().transaction_manager.new_transaction_context(
      hyrise::AutoCommit::No);
  del->set_transaction_context_recursively(txn);
  gt->execute(); val->execute(); scan->execute(); del->execute();

  if (del->execute_failed()) {
    txn->rollback(hyrise::RollbackReason::Conflict);
    return false;
  }
  txn->commit();
  return true;
}

// Update: delete old_row_id, insert new_row_id with same lhs and new rhs.
// Both steps run in one transaction.
static void do_update(Mech m, int32_t old_rid, int32_t new_rid,
                      int32_t lhs, int32_t new_rhs) {
  auto txn = hyrise::Hyrise::get().transaction_manager.new_transaction_context(
      hyrise::AutoCommit::No);

  // DELETE old row
  auto gt   = std::make_shared<hyrise::GetTable>(TABLE_SINGLE);
  auto valop = std::make_shared<hyrise::Validate>(gt);
  auto scan = std::make_shared<hyrise::TableScan>(valop, eq_rowid(old_rid));
  auto del  = std::make_shared<hyrise::Delete>(scan);
  del->set_transaction_context_recursively(txn);
  gt->execute(); valop->execute(); scan->execute(); del->execute();

  if (del->execute_failed()) {
    txn->rollback(hyrise::RollbackReason::Conflict);
    return;
  }

  // INSERT new row
  auto row = std::make_shared<hyrise::Table>(COL_SINGLE, hyrise::TableType::Data);
  row->append({hyrise::AllTypeVariant{new_rid},
               hyrise::AllTypeVariant{lhs},
               hyrise::AllTypeVariant{new_rhs}});
  auto wrapper = std::make_shared<hyrise::TableWrapper>(row);
  auto ins = std::make_shared<hyrise::Insert>(TABLE_SINGLE, wrapper);
  ins->set_transaction_context_recursively(txn);
  wrapper->execute();
  ins->execute();
  txn->commit();
}

static void do_update_multi(int32_t old_rid, int32_t new_rid,
                             int32_t lhs1, int32_t lhs2,
                             int32_t new_r1, int32_t new_r2) {
  auto txn = hyrise::Hyrise::get().transaction_manager.new_transaction_context(
      hyrise::AutoCommit::No);

  auto gt   = std::make_shared<hyrise::GetTable>(TABLE_MULTI);
  auto valop = std::make_shared<hyrise::Validate>(gt);
  auto scan = std::make_shared<hyrise::TableScan>(valop, eq_rowid(old_rid));
  auto del  = std::make_shared<hyrise::Delete>(scan);
  del->set_transaction_context_recursively(txn);
  gt->execute(); valop->execute(); scan->execute(); del->execute();

  if (del->execute_failed()) {
    txn->rollback(hyrise::RollbackReason::Conflict);
    return;
  }

  auto row = std::make_shared<hyrise::Table>(COL_MULTI, hyrise::TableType::Data);
  row->append({hyrise::AllTypeVariant{new_rid},  hyrise::AllTypeVariant{lhs1},
               hyrise::AllTypeVariant{lhs2},      hyrise::AllTypeVariant{new_r1},
               hyrise::AllTypeVariant{new_r2}});
  auto wrapper = std::make_shared<hyrise::TableWrapper>(row);
  auto ins = std::make_shared<hyrise::Insert>(TABLE_MULTI, wrapper);
  ins->set_transaction_context_recursively(txn);
  wrapper->execute();
  ins->execute();
  txn->commit();
}

// ---- table setup -----------------------------------------------------------

static void setup_table(Mech m, int32_t pre_populate, int32_t num_unique_lhs) {
  hyrise::Hyrise::reset();

  g_next_row_id.store(pre_populate);

  const bool multi = is_multi(m);
  auto table = std::make_shared<hyrise::Table>(
      multi ? COL_MULTI : COL_SINGLE,
      hyrise::TableType::Data,
      std::optional<hyrise::ChunkOffset>{hyrise::Chunk::DEFAULT_SIZE},
      hyrise::UseMvcc::Yes);

  for (int32_t i = 0; i < pre_populate; ++i) {
    const int32_t lhs = i % num_unique_lhs;
    if (multi) {
      const auto lhs2 = lhs2_for(lhs), r1 = rhs1_for(lhs), r2 = rhs2_for(lhs, lhs2);
      table->append({hyrise::AllTypeVariant{i},    hyrise::AllTypeVariant{lhs},
                     hyrise::AllTypeVariant{lhs2},  hyrise::AllTypeVariant{r1},
                     hyrise::AllTypeVariant{r2}});
    } else {
      table->append({hyrise::AllTypeVariant{i},
                     hyrise::AllTypeVariant{lhs},
                     hyrise::AllTypeVariant{rhs_for(lhs)}});
    }
  }

  for (auto cid = hyrise::ChunkID{0}; cid < table->chunk_count(); ++cid) {
    auto chunk = table->get_chunk(cid);
    if (!chunk) continue;
    auto mvcc = chunk->mvcc_data();
    for (auto off = hyrise::ChunkOffset{0}; off < chunk->size(); ++off) {
      mvcc->set_begin_cid(off, hyrise::CommitID{0});
      mvcc->set_tid(off, hyrise::TransactionID{0});
    }
    mvcc->max_begin_cid.store(hyrise::CommitID{0});
  }

  if (m != Mech::OFF) {
    const auto d = dep_type(m);
    if (multi) {
      // DVI columns: lhs1=col1, lhs2=col2 -> rhs1=col3, rhs2=col4
      table->set_dependency_validator(
          std::vector<hyrise::ColumnID>{hyrise::ColumnID{1}, hyrise::ColumnID{2}},
          std::vector<hyrise::ColumnID>{hyrise::ColumnID{3}, hyrise::ColumnID{4}}, d);
    } else {
      // DVI columns: lhs=col1 -> rhs=col2
      table->set_dependency_validator(hyrise::ColumnID{1}, hyrise::ColumnID{2}, d);
    }
  }

  hyrise::Hyrise::get().storage_manager.add_table(tname(m), table);
}

// ---- config / args ---------------------------------------------------------

struct Config {
  std::string experiment      = "NAIVE";
  std::string workload        = "delete_only";
  std::string mechanism       = "off";
  double      violation_rate  = 0.0;
  double      duplicate_rate  = 0.0;   // fraction of lhs collisions (0=all unique)
  std::string access_pattern  = "random";
  size_t      size            = 100000;
  size_t      batch_size      = 10000;
  int         threads         = 1;
  int         iterations      = 3;
  std::string output_csv;
};

static Config parse_args(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> std::string {
      if (++i >= argc) throw std::runtime_error("Missing value for " + k);
      return argv[i];
    };
    if      (k == "--experiment")      cfg.experiment      = next();
    else if (k == "--workload")        cfg.workload        = next();
    else if (k == "--mechanism")       cfg.mechanism       = next();
    else if (k == "--violation-rate")  cfg.violation_rate  = std::stod(next());
    else if (k == "--duplicate-rate")  cfg.duplicate_rate  = std::stod(next());
    else if (k == "--access-pattern")  cfg.access_pattern  = next();
    else if (k == "--size")            cfg.size            = std::stoull(next());
    else if (k == "--batch-size")      cfg.batch_size      = std::stoull(next());
    else if (k == "--threads")         cfg.threads         = std::stoi(next());
    else if (k == "--iterations")      cfg.iterations      = std::stoi(next());
    else if (k == "--output-csv")      cfg.output_csv      = next();
    else {
      std::cerr << "Unknown flag: " << k << "\n";
      std::exit(1);
    }
  }
  return cfg;
}

// ---- main ------------------------------------------------------------------

int main(int argc, char** argv) {
  const Config cfg  = parse_args(argc, argv);
  const Mech   mech = parse_mech(cfg.mechanism);
  const bool   multi = is_multi(mech);

  const bool is_insert = cfg.workload == "insert_only";
  const bool is_delete = cfg.workload == "delete_only";
  const bool is_update = cfg.workload == "update_only";
  const bool is_mixed  = cfg.workload == "mixed";

  if (cfg.duplicate_rate < 0.0 || cfg.duplicate_rate >= 1.0) {
    std::cerr << "duplicate-rate must be in [0, 1), got: " << cfg.duplicate_rate << "\n";
    return 1;
  }
  if (cfg.violation_rate < 0.0 || cfg.violation_rate > 1.0) {
    std::cerr << "violation-rate must be in [0, 1], got: " << cfg.violation_rate << "\n";
    return 1;
  }

  const bool seq_access = cfg.access_pattern == "sequential";
  if (!seq_access && cfg.access_pattern != "random") {
    std::cerr << "access-pattern must be random|sequential\n";
    return 1;
  }

  // num_unique_lhs: number of distinct lhs keys in the table.
  // duplicate_rate=0.0 → every row has a unique lhs.
  // duplicate_rate=0.5 → only 50% of rows have unique lhs keys (each appears ~2x).
  const int32_t num_unique_lhs = static_cast<int32_t>(
      std::max(size_t{1},
               static_cast<size_t>(std::ceil(static_cast<double>(cfg.size)
                                             * (1.0 - cfg.duplicate_rate)))));

  const uint64_t viol_threshold =
      static_cast<uint64_t>(std::llround(cfg.violation_rate * 1'000'000.0));

  // Pre-populate full table for non-insert workloads.
  // Insert starts empty; dead rows accumulate from updates/deletes.
  const int32_t pre_pop = is_insert ? 0 : static_cast<int32_t>(cfg.size);

  std::cout << "dvi_txn_benchmark_naive\n"
            << "  experiment:      " << cfg.experiment      << "\n"
            << "  workload:        " << cfg.workload         << "\n"
            << "  mechanism:       " << cfg.mechanism        << "\n"
            << "  violation:       " << cfg.violation_rate   << "\n"
            << "  duplicate_rate:  " << cfg.duplicate_rate   << "\n"
            << "  num_unique_lhs:  " << num_unique_lhs        << "\n"
            << "  access:          " << cfg.access_pattern   << "\n"
            << "  size:            " << cfg.size              << "\n"
            << "  batch_size:      " << cfg.batch_size        << "\n"
            << "  threads:         " << cfg.threads           << "\n"
            << "  pre_pop:         " << pre_pop               << "\n";

  bool csv_header_written = false;
  std::ofstream csv_out;
  if (!cfg.output_csv.empty()) {
    std::ifstream test(cfg.output_csv);
    csv_header_written = test.good() && test.peek() != std::ifstream::traits_type::eof();
    csv_out.open(cfg.output_csv, std::ios::app);
  }

  for (int iter = 0; iter < cfg.iterations; ++iter) {
    std::cout << "\n=== Iteration " << (iter + 1) << " ===\n";
    setup_table(mech, pre_pop, num_unique_lhs);

    double iter_ms = 0.0;
    const size_t num_batches = (cfg.size + cfg.batch_size - 1) / cfg.batch_size;

    for (size_t b = 0; b < num_batches; ++b) {
      const size_t b_start = b * cfg.batch_size;
      const size_t b_end   = std::min((b + 1) * cfg.batch_size, cfg.size);
      const size_t b_n     = b_end - b_start;
      const int    T       = cfg.threads;

      const auto t0 = std::chrono::steady_clock::now();

      std::vector<std::thread> workers;
      workers.reserve(static_cast<size_t>(T));

      for (int t = 0; t < T; ++t) {
        const size_t lo = b_start + static_cast<size_t>(t)     * b_n / static_cast<size_t>(T);
        const size_t hi = b_start + static_cast<size_t>(t + 1) * b_n / static_cast<size_t>(T);

        // Thread t's exclusive slice of the pre-populated row_ids.
        // For delete/update: owns row_ids [range_start, range_end).
        // Uses thread-local current_rowids to track live row_id per slot.
        const int32_t range_start = static_cast<int32_t>(t)     * pre_pop / T;
        const int32_t range_end   = static_cast<int32_t>(t + 1) * pre_pop / T;
        const int32_t range_count = range_end - range_start;

        workers.emplace_back([&, t, lo, hi, range_start, range_count]() {
          // Thread-local current row_id per slot (for delete/update cycling)
          std::vector<int32_t> cur_rid;
          if (!is_insert && range_count > 0) {
            cur_rid.resize(static_cast<size_t>(range_count));
            std::iota(cur_rid.begin(), cur_rid.end(), range_start);
            // Random-access mode: shuffle the visitation order
            if (!seq_access) {
              // Use deterministic per-thread shuffle
              uint64_t seed = splitmix64(static_cast<uint64_t>(t + 1) ^
                                         (static_cast<uint64_t>(iter + 1) << 32));
              for (int32_t j = range_count - 1; j > 0; --j) {
                seed = splitmix64(seed);
                const int32_t k = static_cast<int32_t>(seed % static_cast<uint64_t>(j + 1));
                std::swap(cur_rid[static_cast<size_t>(j)],
                          cur_rid[static_cast<size_t>(k)]);
              }
            }
          }

          for (size_t i = lo; i < hi; ++i) {
            const uint64_t h = splitmix64(i ^ (static_cast<uint64_t>(iter + 1) << 32));
            const bool viol = (splitmix64(i ^ 0xA5A5A5A5ULL) % 1'000'000ULL) < viol_threshold;

            if (is_insert) {
              // INSERT: fresh row_id from global counter, lhs cycles through unique keys
              const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
              const int32_t lhs = seq_access
                  ? static_cast<int32_t>(i % static_cast<size_t>(num_unique_lhs))
                  : static_cast<int32_t>(h % static_cast<uint64_t>(num_unique_lhs));
              if (multi) {
                const auto lhs2 = lhs2_for(lhs);
                const auto r1   = viol ? (rhs1_for(lhs) + 1) : rhs1_for(lhs);
                do_insert_multi(new_rid, lhs, lhs2, r1, rhs2_for(lhs, lhs2));
              } else {
                do_insert(mech, new_rid, lhs, viol ? violation_rhs_for(lhs) : rhs_for(lhs));
              }

            } else if (is_delete && range_count > 0) {
              const auto  j   = static_cast<size_t>(
                  static_cast<int32_t>(i - lo) % range_count);
              const int32_t rid = cur_rid[j];
              // lhs is determined by original slot position (range_start + j)
              const int32_t lhs = static_cast<int32_t>(
                  static_cast<uint32_t>(range_start) +
                  static_cast<uint32_t>(j)) % num_unique_lhs;
              if (do_delete(mech, rid)) {
                const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
                if (multi) {
                  const auto lhs2 = lhs2_for(lhs), r1 = rhs1_for(lhs);
                  do_insert_multi(new_rid, lhs, lhs2, r1, rhs2_for(lhs, lhs2));
                } else {
                  do_insert(mech, new_rid, lhs, rhs_for(lhs));
                }
                cur_rid[j] = new_rid;
              }

            } else if (is_update && range_count > 0) {
              const auto  j   = static_cast<size_t>(
                  static_cast<int32_t>(i - lo) % range_count);
              const int32_t old_rid = cur_rid[j];
              const int32_t lhs = static_cast<int32_t>(
                  static_cast<uint32_t>(range_start) +
                  static_cast<uint32_t>(j)) % num_unique_lhs;
              const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
              const int32_t new_rhs = rhs_for(lhs) + static_cast<int32_t>(i % 2);
              if (multi) {
                const auto lhs2 = lhs2_for(lhs);
                do_update_multi(old_rid, new_rid, lhs, lhs2, new_rhs, new_rhs + lhs2);
              } else {
                do_update(mech, old_rid, new_rid, lhs, new_rhs);
              }
              cur_rid[j] = new_rid;

            } else if (is_mixed && range_count > 0) {
              const int choice = static_cast<int>(i % 10);
              const auto  j   = static_cast<size_t>(
                  static_cast<int32_t>(i - lo) % range_count);
              const int32_t slot_lhs = static_cast<int32_t>(
                  static_cast<uint32_t>(range_start) +
                  static_cast<uint32_t>(j)) % num_unique_lhs;

              if (choice < 7) {
                // INSERT 70%: fresh row with random lhs from unique key space
                const int32_t lhs = seq_access
                    ? static_cast<int32_t>(i % static_cast<size_t>(num_unique_lhs))
                    : static_cast<int32_t>(h % static_cast<uint64_t>(num_unique_lhs));
                const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
                if (multi) {
                  const auto lhs2 = lhs2_for(lhs);
                  const auto r1   = viol ? (rhs1_for(lhs) + 1) : rhs1_for(lhs);
                  do_insert_multi(new_rid, lhs, lhs2, r1, rhs2_for(lhs, lhs2));
                } else {
                  do_insert(mech, new_rid, lhs,
                            viol ? violation_rhs_for(lhs) : rhs_for(lhs));
                }
              } else if (choice < 9) {
                // UPDATE 20%
                const int32_t old_rid = cur_rid[j];
                const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
                const int32_t new_rhs = rhs_for(slot_lhs) + static_cast<int32_t>(i % 2);
                if (multi) {
                  const auto lhs2 = lhs2_for(slot_lhs);
                  do_update_multi(old_rid, new_rid, slot_lhs, lhs2,
                                  new_rhs, new_rhs + lhs2);
                } else {
                  do_update(mech, old_rid, new_rid, slot_lhs, new_rhs);
                }
                cur_rid[j] = new_rid;
              } else {
                // DELETE + reinsert 10%
                const int32_t rid = cur_rid[j];
                if (do_delete(mech, rid)) {
                  const int32_t new_rid = g_next_row_id.fetch_add(1, std::memory_order_relaxed);
                  if (multi) {
                    const auto lhs2 = lhs2_for(slot_lhs), r1 = rhs1_for(slot_lhs);
                    do_insert_multi(new_rid, slot_lhs, lhs2, r1,
                                    rhs2_for(slot_lhs, lhs2));
                  } else {
                    do_insert(mech, new_rid, slot_lhs, rhs_for(slot_lhs));
                  }
                  cur_rid[j] = new_rid;
                }
              }
            }
          }
        });
      }
      for (auto& w : workers) w.join();

      const double batch_ms = std::chrono::duration<double, std::milli>(
          std::chrono::steady_clock::now() - t0).count();
      const double ops_s = batch_ms > 0
          ? (static_cast<double>(b_n) / batch_ms) * 1000.0 : 0.0;
      iter_ms += batch_ms;

      std::cout << "  batch " << (b + 1) << "/" << num_batches
                << " [" << b_start << ".." << b_end << "]: "
                << static_cast<int64_t>(batch_ms) << " ms  ("
                << static_cast<int64_t>(ops_s) << " ops/s)\n";

      if (csv_out.is_open()) {
        if (!csv_header_written) {
          csv_out << "experiment,workload,mechanism,violation_rate,duplicate_rate,"
                     "access_pattern,size,threads,iteration,batch_end,batch_ms,ops_per_s\n";
          csv_header_written = true;
        }
        csv_out << cfg.experiment     << ","
                << cfg.workload        << ","
                << cfg.mechanism       << ","
                << cfg.violation_rate  << ","
                << cfg.duplicate_rate  << ","
                << cfg.access_pattern  << ","
                << cfg.size             << ","
                << cfg.threads          << ","
                << (iter + 1)           << ","
                << b_end                << ","
                << batch_ms             << ","
                << ops_s                << "\n";
        csv_out.flush();
      }
    }

    const double tp = iter_ms > 0
        ? (static_cast<double>(cfg.size) / iter_ms) * 1000.0 : 0.0;
    std::cout << "  total: " << static_cast<int64_t>(iter_ms) << " ms  ("
              << static_cast<int64_t>(tp) << " ops/s)\n";
  }

  hyrise::Hyrise::reset();

  return 0;
}
