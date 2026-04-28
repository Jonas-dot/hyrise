// E2E DVI benchmark -- Q82 break-even analysis (multi-workload)
//
// Measures the break-even point at which the query speedup from an OD-based
// rewrite of TPC-DS Q82 outweighs the cost of maintaining the DVI under four
// distinct write workloads applied to date_dim:
//
//   INSERT_ONLY  -- append new rows with ascending d_date_sk / non-decreasing
//                  d_year (the OD is trivially maintained by ordered inserts).
//
//   UPDATE_ONLY  -- update d_year (the OD RHS column) for pre-populated rows.
//                  Batches are processed from highest to lowest d_date_sk so
//                  the OD stays valid at every intermediate step: already-
//                  updated rows (d_year+1) always have higher d_date_sk than
//                  not-yet-updated rows (still at d_year).
//
//   DELETE_ONLY  -- delete pre-populated rows.  Removing a row cannot create an
//                  OD violation, so G stays 0 throughout.
//
//   MIXED        -- each batch performs 1 insert + 1 update + 1 delete.
//
// For each workload type two scenarios are run on a fresh Hyrise instance:
//
//   Scenario B (baseline, no DVI):
//     - Pre-populate rows where needed (UPDATE / DELETE pools) -- untimed.
//     - Run K batches of the workload operations.
//     - Run Q82 base plan QUERY_RUNS_PER_BATCH times after each batch.
//
//   Scenario A (with DVI):
//     - Same pre-population, then register OD validator on date_dim:
//       d_date_sk (col 0) → d_year (col 6).
//     - Same batches -- DVI maintained throughout.
//     - Run Q82 optimized plan (OD rewrite) in addition to base plan.
//
// d_date_sk pool layout (safe separation, all beyond SF1 range ≈ 2452972):
//
//   WARMUP_SK      = 2452990   (warm-up write target; gap between SF1 end and pools)
//   INSERT_POOL_SK = 2453000   (timed inserts during the benchmark)
//   UPDATE_POOL_SK = 2464000   (pre-populated; d_year updated in-place)
//   DELETE_POOL_SK = 2475000   (pre-populated; rows deleted batch by batch)
//
//   Each pool holds at most MAX_POOL_ROWS rows (= 10000),
//   leaving ample gap (100 SK units) between pools.
//
// Output (per workload type + summary comparison table):
//   write_overhead  = T_ops_A  - T_ops_B    (DVI maintenance cost)
//   query_benefit   = T_base   - T_opt      (speedup per query from rewrite)
//   break_even      = (benefit / total_queries) / (overhead / total_ops)
//                   = "one faster query pays for N write operations"
//
// Build:  cmake target hyriseBenchmarkDVIndexE2E (see src/benchmark/CMakeLists.txt)
// Run:    ./build_release/hyriseBenchmarkDVIndexE2E

#include <array>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <thread>

#ifdef __linux__
#include <fstream>
#endif

#include "benchmark_config.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/index/b_tree/b_tree_olc_index.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

using namespace hyrise;                         // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

// Return the number of physical (non-SMT) cores on NUMA node 0.
// Only compiled and used on Linux (sidon). macOS uses Hyrise's default topology.
//
// Reads /sys/devices/system/node/node0/cpulist to get the CPUs on node 0, then
// counts distinct core_id values to deduplicate HT siblings.
// Falls back to counting package-0 physical cores if cpulist is unavailable.
#ifdef __linux__
static uint32_t physical_cores_numa_node0() {
  // Parse cpulist format: "0-7,16-23" or "0,1,2,..."
  std::ifstream cpulist_f("/sys/devices/system/node/node0/cpulist");
  if (cpulist_f) {
    std::set<int> unique_cores;
    std::string token;
    while (std::getline(cpulist_f, token, ',')) {
      // token is either "N" or "N-M"
      const auto dash = token.find('-');
      const int lo = std::stoi(token);
      const int hi = (dash != std::string::npos) ? std::stoi(token.substr(dash + 1)) : lo;
      for (auto cpu = lo; cpu <= hi; ++cpu) {
        std::ifstream core_f("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/core_id");
        if (core_f) {
          int core_id = -1;
          core_f >> core_id;
          unique_cores.insert(core_id);
        }
      }
    }
    if (!unique_cores.empty()) {
      return static_cast<uint32_t>(unique_cores.size());
    }
  }
  // Fallback: count physical cores on package 0 via sysfs.
  {
    std::set<int> cores;
    for (auto cpu = 0;; ++cpu) {
      const auto base = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/";
      std::ifstream pkg_f(base + "physical_package_id");
      std::ifstream core_f(base + "core_id");
      if (!pkg_f)
        break;
      int pkg = -1, core_id = -1;
      pkg_f >> pkg;
      core_f >> core_id;
      if (pkg == 0) {
        cores.insert(core_id);
      }
    }
    if (!cores.empty()) {
      return static_cast<uint32_t>(cores.size());
    }
  }
  // Last-resort fallback: half of logical CPUs (assumes 2-way SMT).
  const auto logical = std::thread::hardware_concurrency();
  return logical > 0 ? logical / 2 : 1;
}
#endif

// d_date_sk pool layout -- all beyond the SF1 range (ends ~2452972).
// Pools are separated by MAX_POOL_ROWS units so they never overlap even at the
// maximum supported batch count (10 000 batches × 1 op = 10 000 rows/pool).
//
//   INSERT pool  2453000 – 2462999   (timed inserts during the benchmark)
//   UPDATE pool  2464000 – 2473999   (pre-populated; d_fy_year updated in-place)
//   DELETE pool  2475000 – 2484999   (pre-populated; rows deleted batch by batch)
static constexpr auto MAX_POOL_ROWS = int32_t{10000};
static constexpr auto WARMUP_SK = int32_t{2452990};  // safe gap before pools (SF1 ends ~2452972)
static constexpr auto INSERT_POOL_SK = int32_t{2453000};
static constexpr auto UPDATE_POOL_SK = INSERT_POOL_SK + MAX_POOL_ROWS + 1000;  // 2464000
static constexpr auto DELETE_POOL_SK = UPDATE_POOL_SK + MAX_POOL_ROWS + 1000;  // 2475000

// TPC-DS Q82 SQL -- d_date BETWEEN uses a 30-day window within the SF1 data range.
// SF1 date_dim covers 1998-01-02 to 2002-12-28; 2000-05-25 to 2000-06-24 is within range.
static constexpr auto Q82_SQL =
    "SELECT i_item_id, i_item_desc, i_current_price "
    "FROM item, inventory, date_dim, store_sales "
    "WHERE i_current_price BETWEEN 62 AND 92 "
    "AND inv_item_sk = i_item_sk "
    "AND d_date_sk = inv_date_sk "
    "AND d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-06-24' AS date) "
    "AND i_manufact_id IN (129, 270, 821, 423) "
    "AND inv_quantity_on_hand BETWEEN 100 AND 500 "
    "AND ss_item_sk = i_item_sk "
    "GROUP BY i_item_id, i_item_desc, i_current_price "
    "ORDER BY i_item_id LIMIT 100;";

// Workload types.
// MIXED does 1 insert + 1 update + 1 delete per batch regardless of batch_size.
enum class WorkloadType { INSERT_ONLY, UPDATE_ONLY, DELETE_ONLY, MIXED };

static const char* workload_name(WorkloadType wt) {
  switch (wt) {
    case WorkloadType::INSERT_ONLY:
      return "INSERT_ONLY";
    case WorkloadType::UPDATE_ONLY:
      return "UPDATE_ONLY";
    case WorkloadType::DELETE_ONLY:
      return "DELETE_ONLY";
    case WorkloadType::MIXED:
      return "MIXED      ";
  }
  return "UNKNOWN";
}

// Build base and OD-optimized PQPs for Q82.
// Must be called after TPC-DS tables are loaded.
struct Q82Plans {
  std::shared_ptr<AbstractOperator> base_pqp;
  std::shared_ptr<AbstractOperator> opt_pqp;
};

static Q82Plans build_q82_plans() {
  auto pipeline = SQLPipelineBuilder{Q82_SQL}.create_pipeline();
  const auto base_lqp = pipeline.get_optimized_logical_plans().front();
  const auto base_pqp = pipeline.get_physical_plans().front();

  // Build OD-optimized rewrite: replace Semi-join [d_date_sk = inv_date_sk]
  // with a BETWEEN predicate on inv_date_sk using MIN/MAX subqueries over the
  // filtered date_dim.  Valid when the OD d_date_sk ↦ d_year holds (G == 0).
  auto rewrite_lqp = base_lqp->deep_copy();
  auto inventory_node = std::shared_ptr<StoredTableNode>{};

  visit_lqp(rewrite_lqp, [&](const auto& node) {
    const auto stn = std::dynamic_pointer_cast<StoredTableNode>(node);
    if (stn && stn->table_name == "inventory") {
      inventory_node = stn;
    }
    return LQPVisitation::VisitInputs;
  });

  auto rewrite_applied = false;
  visit_lqp(rewrite_lqp, [&](const auto& node) {
    // Match any Semi-join that involves d_date_sk and inv_date_sk -- robust
    // against optimizer operand reordering (don't match by description string).
    const auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
    if (!join_node || join_node->join_mode != JoinMode::Semi) {
      return LQPVisitation::VisitInputs;
    }
    if (join_node->join_predicates().empty()) {
      return LQPVisitation::VisitInputs;
    }
    const auto& pred = static_cast<const BinaryPredicateExpression&>(*join_node->join_predicates().front());
    const auto left_name = pred.left_operand()->as_column_name();
    const auto right_name = pred.right_operand()->as_column_name();
    const auto involves_date_inv = (left_name == "d_date_sk" || left_name == "inv_date_sk") &&
                                   (right_name == "d_date_sk" || right_name == "inv_date_sk");
    if (!involves_date_inv) {
      return LQPVisitation::VisitInputs;
    }

    const auto& join_pred = pred;
    auto left_col = join_pred.left_operand();
    auto right_col = join_pred.right_operand();
    if (left_col->as_column_name() != "inv_date_sk") {
      std::swap(left_col, right_col);
    }

    const auto& right_input = node->right_input();
    const auto mn = min_(right_col);
    const auto mx = max_(right_col);
    const auto agg = AggregateNode::make(expression_vector(), expression_vector(mn, mx), right_input);
    const auto min_proj = ProjectionNode::make(expression_vector(mn), agg);
    const auto max_proj = ProjectionNode::make(expression_vector(mx), agg);

    const auto pred_node =
        PredicateNode::make(between_inclusive_(left_col, lqp_subquery_(min_proj), lqp_subquery_(max_proj)));
    node->set_right_input(nullptr);
    lqp_replace_node(node, pred_node);
    inventory_node->set_prunable_subquery_predicates({pred_node});
    rewrite_applied = true;
    return LQPVisitation::DoNotVisitInputs;
  });

  Assert(rewrite_applied, "Q82 OD rewrite failed: Semi-join [d_date_sk = inv_date_sk] not found in plan");
  return {base_pqp, LQPTranslator{}.translate_node(rewrite_lqp)};
}

// Insert a batch of date rows into date_dim starting at start_sk.
// d_year advances roughly every 365 rows relative to INSERT_POOL_SK,
// keeping the OD d_date_sk → d_year satisfied for all pools.
// d_date is NULL so new rows never match the Q82 BETWEEN predicate.
static void insert_date_batch(int32_t start_sk, int32_t count) {
  for (auto i = int32_t{0}; i < count; ++i) {
    const auto sk = start_sk + i;
    const auto year = 2003 + (sk - INSERT_POOL_SK) / 365;
    const auto date_id = "E2E_" + std::to_string(sk);

    // 28 columns: d_date_sk, d_date_id, d_date(NULL), d_month_seq(NULL),
    // d_week_seq(NULL), d_quarter_seq(NULL), d_year, d_dow(NULL), d_moy(NULL),
    // d_dom(NULL), d_qoy(NULL), d_fy_year, d_fy_quarter_seq(NULL),
    // d_fy_week_seq(NULL), d_day_name(NULL), d_quarter_name(NULL),
    // d_holiday(NULL), d_weekend(NULL), d_following_holiday(NULL),
    // d_first_dom(NULL), d_last_dom(NULL), d_same_day_ly(NULL),
    // d_same_day_lq(NULL), d_current_day(NULL), d_current_week(NULL),
    // d_current_month(NULL), d_current_quarter(NULL), d_current_year(NULL)
    const auto sql = "INSERT INTO date_dim VALUES (" + std::to_string(sk) + ", '" + date_id +
                     "', "
                     "NULL, NULL, NULL, NULL, " +
                     std::to_string(year) +
                     ", "
                     "NULL, NULL, NULL, NULL, " +
                     std::to_string(year) +
                     ", "
                     "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                     "NULL, NULL, NULL, NULL, NULL)";

    SQLPipelineBuilder{sql}.create_pipeline().get_result_table();
  }
}

// Update d_year (the OD RHS column) for a batch of pre-populated rows.
//
// Rows are processed in DESCENDING d_date_sk order so the OD
// d_date_sk → d_year stays satisfied at every intermediate step:
// already-updated rows (d_year+1) always have higher d_date_sk than
// not-yet-updated rows (still at d_year).
static void update_date_batch(int32_t start_sk, int32_t count) {
  for (auto i = count - 1; i >= 0; --i) {
    const auto sk = start_sk + i;
    const auto new_year = 2003 + (sk - INSERT_POOL_SK) / 365 + 1;
    const auto sql =
        "UPDATE date_dim SET d_year = " + std::to_string(new_year) + " WHERE d_date_sk = " + std::to_string(sk);
    SQLPipelineBuilder{sql}.create_pipeline().get_result_table();
  }
}

// Delete a batch of pre-populated rows from date_dim.
// Deleting rows cannot create an OD violation -- G stays 0 throughout.
static void delete_date_batch(int32_t start_sk, int32_t count) {
  for (auto i = int32_t{0}; i < count; ++i) {
    const auto sk = start_sk + i;
    const auto sql = "DELETE FROM date_dim WHERE d_date_sk = " + std::to_string(sk);
    SQLPipelineBuilder{sql}.create_pipeline().get_result_table();
  }
}

// Per-scenario timing results (all durations in nanoseconds).
struct ScenarioResult {
  std::chrono::nanoseconds insert_time{0};
  std::chrono::nanoseconds update_time{0};
  std::chrono::nanoseconds delete_time{0};
  std::chrono::nanoseconds query_base_time{0};
  std::chrono::nanoseconds query_opt_time{0};
};

// Run one scenario (with or without DVI) for the given workload type.
//
// Pre-population of the UPDATE / DELETE pools happens before DVI registration
// and outside the timing loop, so it does not inflate the measured overhead.
static ScenarioResult run_scenario(WorkloadType wt, bool with_dvi, int32_t num_batches, int32_t batch_size,
                                   int32_t query_runs_per_batch) {
  // On Linux (sidon): pin workers to the physical cores of NUMA node 0 to avoid
  // cross-node memory traffic and to keep results comparable across SMT configs.
  // On macOS: use Hyrise's default topology (single NUMA domain, no pinning needed).
#ifdef __linux__
  const auto cores = physical_cores_numa_node0();
  std::cout << "  [scheduler] using " << cores << " physical core(s) on NUMA node 0\n";
  Hyrise::get().topology.use_numa_topology(cores);
#endif

  const auto scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(scheduler);

  // Operations per batch per type.
  // For MIXED: 1 insert + 1 update + 1 delete per batch regardless of batch_size.
  // For single-type workloads: batch_size operations per batch.
  const auto ops = (wt == WorkloadType::MIXED) ? std::max(int32_t{1}, batch_size / 3) : batch_size;

  // --- Pre-populate UPDATE / DELETE pools (untimed, before DVI registration) ---
  const auto pool_rows = num_batches * ops;
  if (wt == WorkloadType::UPDATE_ONLY || wt == WorkloadType::MIXED) {
    insert_date_batch(UPDATE_POOL_SK, pool_rows);
  }
  if (wt == WorkloadType::DELETE_ONLY || wt == WorkloadType::MIXED) {
    insert_date_batch(DELETE_POOL_SK, pool_rows);
  }

  // --- Register DVI after pre-population so the validator sees a clean table ---
  if (with_dvi) {
    auto date_dim = Hyrise::get().storage_manager.get_table("date_dim");
    date_dim->set_dependency_validator(ColumnID{0}, ColumnID{6}, DependencyType::OD);
  }

  const auto txn = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  const auto plans = build_q82_plans();
  auto result = ScenarioResult{};
  auto timer = Timer{};

  // --- Warm-up: one insert + one query (untimed) ---
  // Exercises the SQL planner, MVCC transaction manager, and chunk allocator
  // before measurement starts so both Scenario B and A begin from an equally
  // warm state.  WARMUP_SK sits in the gap between SF1 data and the timed pools.
  // We only insert (never update d_year) to keep the OD d_date_sk → d_year intact.
  {
    insert_date_batch(WARMUP_SK, 1);
    const auto base_copy = plans.base_pqp->deep_copy();
    base_copy->set_transaction_context_recursively(txn);
    const auto base_tasks = OperatorTask::make_tasks_from_operator(base_copy).first;
    scheduler->schedule_and_wait_for_tasks(base_tasks);
  }

  for (auto batch = int32_t{0}; batch < num_batches; ++batch) {
    // --- Inserts (INSERT_ONLY and MIXED) ---
    if (wt == WorkloadType::INSERT_ONLY || wt == WorkloadType::MIXED) {
      timer.lap();
      insert_date_batch(INSERT_POOL_SK + batch * ops, ops);
      result.insert_time += timer.lap();
    }

    // --- Updates to d_year (UPDATE_ONLY and MIXED) ---
    // Process the pool from the end backward so the OD is preserved at every step:
    //   batch 0 → last   `ops` rows of the pool (highest d_date_sk)
    //   batch 1 → second-to-last `ops` rows
    //   ...
    if (wt == WorkloadType::UPDATE_ONLY || wt == WorkloadType::MIXED) {
      const auto update_start = UPDATE_POOL_SK + (num_batches - 1 - batch) * ops;
      timer.lap();
      update_date_batch(update_start, ops);
      result.update_time += timer.lap();
    }

    // --- Deletes (DELETE_ONLY and MIXED) ---
    if (wt == WorkloadType::DELETE_ONLY || wt == WorkloadType::MIXED) {
      timer.lap();
      delete_date_batch(DELETE_POOL_SK + batch * ops, ops);
      result.delete_time += timer.lap();
    }

    // --- Verify OD is still intact when DVI is active ---
    if (with_dvi) {
      const auto& deps = Hyrise::get().storage_manager.get_table("date_dim")->dependency_validators();
      Assert(!deps.empty() && deps[0].index->global_violation_count() == 0,
             "OD violated -- check update ordering or pool layout");
    }

    // --- Queries ---
    for (auto q = 0; q < query_runs_per_batch; ++q) {
      // Base plan -- measured in both scenarios for the benefit calculation.
      const auto base_copy = plans.base_pqp->deep_copy();
      base_copy->set_transaction_context_recursively(txn);
      const auto base_tasks = OperatorTask::make_tasks_from_operator(base_copy).first;
      timer.lap();
      scheduler->schedule_and_wait_for_tasks(base_tasks);
      result.query_base_time += timer.lap();

      // Optimized plan -- only valid with DVI active (G == 0 guaranteed).
      if (with_dvi) {
        const auto opt_copy = plans.opt_pqp->deep_copy();
        opt_copy->set_transaction_context_recursively(txn);
        const auto opt_tasks = OperatorTask::make_tasks_from_operator(opt_copy).first;
        timer.lap();
        scheduler->schedule_and_wait_for_tasks(opt_tasks);
        result.query_opt_time += timer.lap();
      }
    }
  }

  scheduler->finish();
  return result;
}

// Print a result block for one workload type.
// Returns the break-even ratio (or -1 if overhead ≤ 0).
static double print_workload_result(WorkloadType wt, const ScenarioResult& result_a, const ScenarioResult& result_b,
                                    int32_t total_ops, int32_t total_queries) {
  // Total write time per scenario for this workload type.
  const auto ops_a_ns = result_a.insert_time + result_a.update_time + result_a.delete_time;
  const auto ops_b_ns = result_b.insert_time + result_b.update_time + result_b.delete_time;

  const auto ops_a_ms = static_cast<double>(ops_a_ns.count()) / 1e6;
  const auto ops_b_ms = static_cast<double>(ops_b_ns.count()) / 1e6;
  const auto base_ms = static_cast<double>(result_b.query_base_time.count()) / 1e6;
  const auto opt_ms = static_cast<double>(result_a.query_opt_time.count()) / 1e6;
  const auto ovhd_ms = ops_a_ms - ops_b_ms;
  const auto benef_ms = base_ms - opt_ms;

  std::cout << "\n  Workload: " << workload_name(wt) << "\n";
  std::cout << "  ------------------------------------------------\n";
  std::cout << "  Write time  (no DVI)  : " << ops_b_ms << " ms\n";
  std::cout << "  Write time  (DVI)     : " << ops_a_ms << " ms\n";
  std::cout << "  Write overhead        : " << ovhd_ms << " ms  (" << (ops_b_ms > 0 ? ovhd_ms / ops_b_ms * 100.0 : 0.0)
            << " %)\n";
  std::cout << "  Query time  (base)    : " << base_ms << " ms total\n";
  std::cout << "  Query time  (opt)     : " << opt_ms << " ms total\n";
  std::cout << "  Query benefit         : " << benef_ms << " ms total  ("
            << (base_ms > 0 ? benef_ms / base_ms * 100.0 : 0.0) << " %)\n";

  if (ovhd_ms > 0.0) {
    const auto bpq = benef_ms / total_queries;
    const auto opo = ovhd_ms / total_ops;
    const auto bev = bpq / opo;
    std::cout << "  Break-even ratio      : 1 query pays for ~" << static_cast<int>(bev) << " ops\n";
    return bev;
  }

  std::cout << "  Break-even ratio      : write overhead unmeasurably small\n";
  return -1.0;
}

// main
int main(int argc, char* argv[]) {
  // Scale factor 1 for local runs; use 10 on sidon for thesis results.
  constexpr auto SCALE_FACTOR = 1;
  // Each batch contains exactly 1 write operation (per operation type).
  // For MIXED each batch does 1 insert + 1 update + 1 delete.
  constexpr auto BATCH_SIZE = int32_t{1};
  // Q82 executions per batch.
  constexpr auto QUERY_RUNS_PER_BATCH = 1;

  // Parse --batches N and --output-csv PATH from argv.
  auto num_batches = int32_t{100};
  auto output_csv = std::string{};
  for (auto i = 1; i < argc - 1; ++i) {
    if (std::strcmp(argv[i], "--batches") == 0) {
      num_batches = static_cast<int32_t>(std::atoi(argv[i + 1]));
    } else if (std::strcmp(argv[i], "--output-csv") == 0) {
      output_csv = argv[i + 1];
    }
  }

  const auto total_ops = num_batches * BATCH_SIZE;  // per operation type
  const auto total_queries = num_batches * QUERY_RUNS_PER_BATCH;

  constexpr std::array workloads = {
      WorkloadType::INSERT_ONLY,
      WorkloadType::UPDATE_ONLY,
      WorkloadType::DELETE_ONLY,
      WorkloadType::MIXED,
  };

  // Collect per-workload break-even ratios and raw results for CSV output.
  std::array<double, workloads.size()> break_even_ratios{};
  std::array<ScenarioResult, workloads.size()> all_results_a{};
  std::array<ScenarioResult, workloads.size()> all_results_b{};

  for (auto i = std::size_t{0}; i < workloads.size(); ++i) {
    const auto wt = workloads[i];
    std::cout << "\n======================================================"
              << "======\n";
    std::cout << "  Workload: " << workload_name(wt) << "\n";
    std::cout << "========================================================"
              << "====\n";

    // --- Scenario B: no DVI ---
    std::cout << "  [B] loading TPC-DS (no DVI)...\n";
    Hyrise::reset();
    {
      const auto config = std::make_shared<BenchmarkConfig>(Chunk::DEFAULT_SIZE, true);
      // Suppress verbose table-loading output from TPCDSTableGenerator.
      std::ofstream null_out("/dev/null");
      auto* old_buf = std::cout.rdbuf(null_out.rdbuf());
      TPCDSTableGenerator{SCALE_FACTOR, config}.generate_and_store();
      std::cout.rdbuf(old_buf);
    }
    std::cout << "  [B] loading done\n";
    const auto result_b = run_scenario(wt, false, num_batches, BATCH_SIZE, QUERY_RUNS_PER_BATCH);

    // --- Scenario A: with DVI ---
    std::cout << "  [A] loading TPC-DS (with DVI)...\n";
    Hyrise::reset();
    {
      const auto config = std::make_shared<BenchmarkConfig>(Chunk::DEFAULT_SIZE, true);
      std::ofstream null_out("/dev/null");
      auto* old_buf = std::cout.rdbuf(null_out.rdbuf());
      TPCDSTableGenerator{SCALE_FACTOR, config}.generate_and_store();
      std::cout.rdbuf(old_buf);
    }
    std::cout << "  [A] loading done\n";
    const auto result_a = run_scenario(wt, true, num_batches, BATCH_SIZE, QUERY_RUNS_PER_BATCH);

    all_results_b[i] = result_b;
    all_results_a[i] = result_a;
    break_even_ratios[i] = print_workload_result(wt, result_a, result_b, total_ops, total_queries);
  }

  // Summary comparison table (stdout)
  std::cout << "\n\n";
  std::cout << "========================================================\n";
  std::cout << "  E2E DVI Benchmark -- Q82 Break-Even Summary\n";
  std::cout << "  Scale factor : " << SCALE_FACTOR << "   " << num_batches << " batches × " << BATCH_SIZE << " ops   "
            << total_queries << " query runs\n";
  std::cout << "========================================================\n";
  std::cout << "  Workload       Break-even ratio (ops per query)\n";
  std::cout << "  ---------      ------------------------\n";
  for (auto i = std::size_t{0}; i < workloads.size(); ++i) {
    std::cout << "  " << workload_name(workloads[i]) << "   ";
    if (break_even_ratios[i] > 0.0) {
      std::cout << "~" << static_cast<int>(break_even_ratios[i]) << "\n";
    } else {
      std::cout << "overhead negligible\n";
    }
  }
  std::cout << "========================================================\n";

  // CSV output (one row per workload type)
  // workload, batches, ops_no_dvi_ms, ops_dvi_ms, overhead_ms, overhead_pct,
  // query_base_ms, query_opt_ms, benefit_ms, benefit_pct, break_even_ratio
  auto emit_csv = [&](std::ostream& out, bool header) {
    if (header) {
      out << "workload,batches,scale_factor,"
          << "ops_no_dvi_ms,ops_dvi_ms,overhead_ms,overhead_pct,"
          << "query_base_ms,query_opt_ms,benefit_ms,benefit_pct,break_even_ratio\n";
    }
    for (auto i = std::size_t{0}; i < workloads.size(); ++i) {
      const auto& ra = all_results_a[i];
      const auto& rb = all_results_b[i];

      const auto ops_a = static_cast<double>((ra.insert_time + ra.update_time + ra.delete_time).count()) / 1e6;
      const auto ops_b = static_cast<double>((rb.insert_time + rb.update_time + rb.delete_time).count()) / 1e6;
      const auto base = static_cast<double>(rb.query_base_time.count()) / 1e6;
      const auto opt = static_cast<double>(ra.query_opt_time.count()) / 1e6;
      const auto ovhd = ops_a - ops_b;
      const auto benef = base - opt;

      // Strip trailing spaces from workload name for CSV.
      auto wname = std::string{workload_name(workloads[i])};
      while (!wname.empty() && wname.back() == ' ')
        wname.pop_back();

      out << wname << "," << num_batches << "," << SCALE_FACTOR << "," << ops_b << "," << ops_a << "," << ovhd << ","
          << (ops_b > 0 ? ovhd / ops_b * 100.0 : 0.0) << "," << base << "," << opt << "," << benef << ","
          << (base > 0 ? benef / base * 100.0 : 0.0) << "," << (break_even_ratios[i] > 0.0 ? break_even_ratios[i] : 0.0)
          << "\n";
    }
  };

  emit_csv(std::cout, true);

  if (!output_csv.empty()) {
    // Append if file exists (supports multiple iterations), write header only when new.
    const auto file_exists = std::ifstream{output_csv}.good();
    std::ofstream csv_f{output_csv, std::ios::app};
    if (csv_f) {
      emit_csv(csv_f, !file_exists);
      std::cout << "Results appended to: " << output_csv << "\n";
    } else {
      std::cerr << "WARNING: could not open output CSV: " << output_csv << "\n";
    }
  }

  Hyrise::reset();

  return 0;
}
