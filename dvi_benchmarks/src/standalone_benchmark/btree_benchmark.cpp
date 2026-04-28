/**
 * Micro-Benchmark: OLC B-Tree with Dependency Validation Index (DVI)
 *
 * Experiment M0: OD full-factorial sweep
 *   Mechanisms : od_single, od_multi
 *   Varies     : operation × violation_rate × duplicate_rate × threads × access_pattern
 *   Fixed at   : 10M operations, checkpointed every 100k ops
 *
 * Experiment M1: Ablation study – all 5 mechanisms head-to-head
 *   Mechanisms : off, fd_single, fd_multi, od_single, od_multi
 *   Fixed at   : 10M ops, 10% duplicate-rate, 0% violation, threads 1+16, checkpointed every 100k
 *
 * Usage:
 *   btree_benchmark --experiment M0 [options]
 *   btree_benchmark --experiment M1 [options]
 *
 * Options:
 *   --experiment        M0|M1                                   (default: M0)
 *   --mechanism         off|fd_single|fd_multi|od_single|od_multi|all
 *   --operation         insert|lookup|delete|all                (default: all)
 *   --violation-rates   r1,r2,...  (0.0–1.0)                   (default: 0.0,0.1,0.2,1.0)
 *   --duplicate-rates   r1,r2,...  (0.0–1.0)                   (default: 0.1,0.0)
 *   --sizes             s1,s2,...  (total op count)             (default: 10000000)
 *   --checkpoint-interval N       (record a row every N ops)   (default: 0 = one row per run)
 *   --threads           t1,t2,...                               (default: 1,2,4,8,16,32,64)
 *   --access-pattern    random|sequential|both                  (default: both)
 *   --iterations        N                                       (default: 3)
 *   --output            FILE.csv                                (default: results.csv)
 *
 * Output CSV schema:
 *   experiment,operation,mechanism,violation_rate,duplicate_rate,size,
 *   checkpoint_ops,access_pattern,threads,iteration,time_ms,throughput_ops_per_s
 *
 *   checkpoint_ops: cumulative ops at this checkpoint (= size when not checkpointing)
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../standalone_btree/with_dependency_validation/btree_olc.hpp"
#include "btree_olc.hpp"

// Enums

enum class Mechanism { OFF, FD_SINGLE, FD_MULTI, OD_SINGLE, OD_MULTI };
enum class Operation { INSERT, LOOKUP, DELETE };
enum class AccessPattern { RANDOM, SEQUENTIAL };

std::string mechanism_to_string(Mechanism m) {
  switch (m) {
    case Mechanism::OFF:
      return "off";
    case Mechanism::FD_SINGLE:
      return "fd_single";
    case Mechanism::FD_MULTI:
      return "fd_multi";
    case Mechanism::OD_SINGLE:
      return "od_single";
    case Mechanism::OD_MULTI:
      return "od_multi";
  }
  return "unknown";
}

std::string operation_to_string(Operation op) {
  switch (op) {
    case Operation::INSERT:
      return "insert";
    case Operation::LOOKUP:
      return "lookup";
    case Operation::DELETE:
      return "delete";
  }
  return "unknown";
}

std::string pattern_to_string(AccessPattern p) {
  return p == AccessPattern::RANDOM ? "random" : "sequential";
}

// Data Entry

struct Entry {
  uint64_t determinant;  // A (or composite A for multi-column)
  uint64_t dependent;    // B
};

// Data Generation

std::vector<Entry> generate_fd_entries(size_t count, double violation_rate, size_t num_determinants, bool multi_col,
                                       uint64_t seed) {
  std::vector<Entry> entries;
  entries.reserve(count);

  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> viol_dist(0.0, 1.0);
  std::unordered_map<uint64_t, uint64_t> canonical;

  size_t col_range =
      multi_col ? static_cast<size_t>(std::ceil(std::sqrt(static_cast<double>(num_determinants)))) : num_determinants;
  std::uniform_int_distribution<uint64_t> col1_dist(0, col_range - 1);
  std::uniform_int_distribution<uint64_t> col2_dist(0, col_range - 1);

  for (size_t i = 0; i < count; ++i) {
    uint64_t det;
    if (multi_col) {
      det = (col1_dist(rng) << 32) | col2_dist(rng);
    } else {
      det = col1_dist(rng);
    }

    uint64_t dep;
    auto it = canonical.find(det);
    if (it == canonical.end()) {
      dep = rng();
      canonical[det] = dep;
    } else {
      if (viol_dist(rng) < violation_rate) {
        dep = it->second + (rng() % 1000) + 1;
      } else {
        dep = it->second;
      }
    }
    entries.push_back({det, dep});
  }
  return entries;
}

std::vector<Entry> generate_od_entries(size_t count, double violation_rate, bool multi_col, uint64_t seed) {
  std::vector<Entry> entries;
  entries.reserve(count);

  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> viol_dist(0.0, 1.0);

  for (size_t i = 0; i < count; ++i) {
    uint64_t det;
    if (multi_col) {
      uint64_t hi = i >> 16;
      uint64_t lo = i & 0xFFFF;
      det = (hi << 32) | lo;
    } else {
      det = i;
    }
    uint64_t dep = i;
    if (i > 0 && viol_dist(rng) < violation_rate) {
      dep = rng() % i;
    }
    entries.push_back({det, dep});
  }

  std::shuffle(entries.begin(), entries.end(), rng);
  return entries;
}

std::vector<Entry> generate_fd_entries_sequential(size_t count, double violation_rate, size_t num_determinants,
                                                  bool multi_col, uint64_t seed) {
  std::vector<Entry> entries;
  entries.reserve(count);

  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> viol_dist(0.0, 1.0);

  size_t col_range =
      multi_col ? static_cast<size_t>(std::ceil(std::sqrt(static_cast<double>(num_determinants)))) : num_determinants;

  std::unordered_map<uint64_t, uint64_t> canonical;

  for (size_t i = 0; i < count; ++i) {
    uint64_t det;
    if (multi_col) {
      uint64_t bucket = i % (col_range * col_range);
      det = ((bucket / col_range) << 32) | (bucket % col_range);
    } else {
      det = i % col_range;
    }

    uint64_t dep;
    auto it = canonical.find(det);
    if (it == canonical.end()) {
      dep = i * 7 + 1000;
      canonical[det] = dep;
    } else {
      dep = (viol_dist(rng) < violation_rate) ? it->second + (rng() % 100) + 1 : it->second;
    }
    entries.push_back({det, dep});
  }
  return entries;
}

std::vector<Entry> generate_od_entries_sequential(size_t count, double violation_rate, bool multi_col, uint64_t seed) {
  std::vector<Entry> entries;
  entries.reserve(count);

  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> viol_dist(0.0, 1.0);

  for (size_t i = 0; i < count; ++i) {
    uint64_t det;
    if (multi_col) {
      uint64_t hi = i >> 16;
      uint64_t lo = i & 0xFFFF;
      det = (hi << 32) | lo;
    } else {
      det = i;
    }
    uint64_t dep = (i > 0 && viol_dist(rng) < violation_rate) ? rng() % i : i;
    entries.push_back({det, dep});
  }
  return entries;
}

std::vector<Entry> generate_lookup_keys(const std::vector<Entry>& base, AccessPattern pattern, uint64_t seed) {
  std::vector<Entry> keys = base;
  if (pattern == AccessPattern::RANDOM) {
    std::mt19937_64 rng(seed);
    std::shuffle(keys.begin(), keys.end(), rng);
  }
  return keys;
}

// Timing

using Clock = std::chrono::high_resolution_clock;
using DurationMs = std::chrono::duration<double, std::milli>;

struct RunResult {
  double time_ms;
  size_t ops;

  double throughput_ops_per_s() const {
    return time_ms > 0.0 ? (static_cast<double>(ops) / time_ms) * 1000.0 : 0.0;
  }
};

// OLC key helpers

void to_olc_key(uint64_t k, btree_olc::u8* buf) {
  for (int i = 7; i >= 0; --i)
    buf[7 - i] = static_cast<uint8_t>((k >> (i * 8)) & 0xFF);
}

// Batch helper: time one slice [begin, end) of insert/lookup/delete
// on an externally-owned OFF tree.

static RunResult time_insert_batch_off(btree_olc::BTree& tree, const std::vector<Entry>& entries, size_t begin,
                                       size_t end, size_t num_threads) {
  const size_t batch_n = end - begin;
  const size_t per_thread = (batch_n + num_threads - 1) / num_threads;
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    size_t tb = begin + t * per_thread;
    size_t te = std::min(tb + per_thread, end);
    threads.emplace_back([&tree, &entries, tb, te]() {
      btree_olc::u8 kb[8];
      for (size_t i = tb; i < te; ++i) {
        to_olc_key(entries[i].determinant, kb);
        uint64_t v = entries[i].dependent;
        tree.insert(kb, 8, reinterpret_cast<btree_olc::u8*>(&v), sizeof(v));
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

static RunResult time_insert_batch_val(btree_olc::DependencyValidatingBTree& tree, const std::vector<Entry>& entries,
                                       size_t begin, size_t end, size_t num_threads) {
  const size_t batch_n = end - begin;
  const size_t per_thread = (batch_n + num_threads - 1) / num_threads;
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    size_t tb = begin + t * per_thread;
    size_t te = std::min(tb + per_thread, end);
    threads.emplace_back([&tree, &entries, tb, te]() {
      for (size_t i = tb; i < te; ++i) {
        btree_olc::DVTransactionContext txn(tree);
        txn.insert(entries[i].determinant, entries[i].dependent);
        txn.commit();
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

static RunResult time_lookup_batch_off(btree_olc::BTree& tree, const std::vector<Entry>& lookup_keys, size_t begin,
                                       size_t end, size_t num_threads) {
  const size_t batch_n = end - begin;
  std::atomic<size_t> idx{begin};
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([&tree, &lookup_keys, &idx, end]() {
      btree_olc::u8 kb[8];
      size_t i;
      while ((i = idx.fetch_add(1)) < end) {
        to_olc_key(lookup_keys[i].determinant, kb);
        tree.lookup(kb, 8, [](const btree_olc::u8*, btree_olc::u16) {});
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

static RunResult time_lookup_batch_val(btree_olc::DependencyValidatingBTree& tree,
                                       const std::vector<Entry>& lookup_keys, size_t begin, size_t end,
                                       size_t num_threads) {
  const size_t batch_n = end - begin;
  std::atomic<size_t> idx{begin};
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([&tree, &lookup_keys, &idx, end]() {
      size_t i;
      while ((i = idx.fetch_add(1)) < end) {
        tree.lookup(lookup_keys[i].determinant, lookup_keys[i].dependent);
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

static RunResult time_delete_batch_off(btree_olc::BTree& tree, const std::vector<Entry>& delete_keys, size_t begin,
                                       size_t end, size_t num_threads) {
  const size_t batch_n = end - begin;
  std::atomic<size_t> idx{begin};
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([&tree, &delete_keys, &idx, end]() {
      btree_olc::u8 kb[8];
      size_t i;
      while ((i = idx.fetch_add(1)) < end) {
        to_olc_key(delete_keys[i].determinant, kb);
        tree.remove(kb, 8);
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

static RunResult time_delete_batch_val(btree_olc::DependencyValidatingBTree& tree,
                                       const std::vector<Entry>& delete_keys, size_t begin, size_t end,
                                       size_t num_threads) {
  const size_t batch_n = end - begin;
  std::atomic<size_t> idx{begin};
  std::vector<std::thread> threads;
  auto start = Clock::now();
  for (size_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([&tree, &delete_keys, &idx, end]() {
      size_t i;
      while ((i = idx.fetch_add(1)) < end) {
        btree_olc::DVTransactionContext txn(tree);
        txn.remove(delete_keys[i].determinant, delete_keys[i].dependent);
        txn.commit();
      }
    });
  }
  for (auto& th : threads)
    th.join();
  return {DurationMs(Clock::now() - start).count(), batch_n};
}

// Checkpoint-aware run functions
//
// When interval == 0 → one result (full run).
// When interval > 0  → one result per [b*interval, (b+1)*interval) batch.

std::vector<RunResult> run_insert(Mechanism mech, const std::vector<Entry>& entries, size_t num_threads,
                                  size_t interval) {
  const size_t n = entries.size();
  const size_t step = (interval == 0) ? n : interval;
  const size_t num_batches = (n + step - 1) / step;
  std::vector<RunResult> results;
  results.reserve(num_batches);

  if (mech == Mechanism::OFF) {
    btree_olc::BTree tree;
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_insert_batch_off(tree, entries, b * step, std::min((b + 1) * step, n), num_threads));
    }
  } else {
    bool is_fd = (mech == Mechanism::FD_SINGLE || mech == Mechanism::FD_MULTI);
    btree_olc::DependencyValidatingBTree tree(is_fd ? btree_olc::DependencyType::FD : btree_olc::DependencyType::OD);
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_insert_batch_val(tree, entries, b * step, std::min((b + 1) * step, n), num_threads));
    }
  }
  return results;
}

std::vector<RunResult> run_lookup(Mechanism mech, const std::vector<Entry>& entries,
                                  const std::vector<Entry>& lookup_keys, size_t num_threads, size_t interval) {
  const size_t n = lookup_keys.size();
  const size_t step = (interval == 0) ? n : interval;
  const size_t num_batches = (n + step - 1) / step;
  std::vector<RunResult> results;
  results.reserve(num_batches);

  if (mech == Mechanism::OFF) {
    // Pre-populate tree (not timed)
    std::cout << "    [PREPOP] Pre-populating OFF tree with " << entries.size() << " entries for lookup ..."
              << std::flush;
    btree_olc::BTree tree;
    {
      btree_olc::u8 kb[8];
      for (const auto& e : entries) {
        to_olc_key(e.determinant, kb);
        uint64_t v = e.dependent;
        tree.insert(kb, 8, reinterpret_cast<btree_olc::u8*>(&v), sizeof(v));
      }
    }
    std::cout << " done." << std::endl;
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_lookup_batch_off(tree, lookup_keys, b * step, std::min((b + 1) * step, n), num_threads));
    }
  } else {
    // Pre-populate tree (not timed)
    std::cout << "    [PREPOP] Pre-populating DVI tree with " << entries.size()
              << " entries for lookup (direct insert) ..." << std::flush;
    bool is_fd = (mech == Mechanism::FD_SINGLE || mech == Mechanism::FD_MULTI);
    btree_olc::DependencyValidatingBTree tree(is_fd ? btree_olc::DependencyType::FD : btree_olc::DependencyType::OD);
    for (const auto& e : entries) {
      tree.insert(e.determinant, e.dependent);
    }
    std::cout << " done." << std::endl;
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_lookup_batch_val(tree, lookup_keys, b * step, std::min((b + 1) * step, n), num_threads));
    }
  }
  return results;
}

std::vector<RunResult> run_delete(Mechanism mech, const std::vector<Entry>& entries,
                                  const std::vector<Entry>& delete_keys, size_t num_threads, size_t interval) {
  const size_t n = delete_keys.size();
  const size_t step = (interval == 0) ? n : interval;
  const size_t num_batches = (n + step - 1) / step;
  std::vector<RunResult> results;
  results.reserve(num_batches);

  if (mech == Mechanism::OFF) {
    // Pre-populate tree (not timed)
    std::cout << "    [PREPOP] Pre-populating OFF tree with " << entries.size() << " entries for delete ..."
              << std::flush;
    btree_olc::BTree tree;
    {
      btree_olc::u8 kb[8];
      for (const auto& e : entries) {
        to_olc_key(e.determinant, kb);
        uint64_t v = e.dependent;
        tree.insert(kb, 8, reinterpret_cast<btree_olc::u8*>(&v), sizeof(v));
      }
    }
    std::cout << " done." << std::endl;
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_delete_batch_off(tree, delete_keys, b * step, std::min((b + 1) * step, n), num_threads));
    }
  } else {
    // Pre-populate tree (not timed) -- use direct insert (no TransactionContext)
    // to avoid per-entry std::set allocation and boundary-check overhead.
    std::cout << "    [PREPOP] Pre-populating DVI tree with " << entries.size()
              << " entries for delete (direct insert, no TxnCtx) ..." << std::flush;
    bool is_fd = (mech == Mechanism::FD_SINGLE || mech == Mechanism::FD_MULTI);
    btree_olc::DependencyValidatingBTree tree(is_fd ? btree_olc::DependencyType::FD : btree_olc::DependencyType::OD);
    for (const auto& e : entries) {
      tree.insert(e.determinant, e.dependent);
    }
    std::cout << " done." << std::endl;
    for (size_t b = 0; b < num_batches; ++b) {
      results.push_back(time_delete_batch_val(tree, delete_keys, b * step, std::min((b + 1) * step, n), num_threads));
    }
  }
  return results;
}

// CSV Writer

class CSVWriter {
 public:
  explicit CSVWriter(const std::string& filename) : _file(filename, std::ios::app) {
    _file.seekp(0, std::ios::end);
    if (_file.tellp() == 0) {
      _file << "experiment,operation,mechanism,violation_rate,duplicate_rate,"
               "size,checkpoint_ops,access_pattern,threads,iteration,"
               "time_ms,throughput_ops_per_s\n";
    }
  }

  void write(const std::string& experiment, const std::string& operation, const std::string& mechanism,
             double violation_rate, double duplicate_rate, size_t size, size_t checkpoint_ops,
             const std::string& access_pattern, size_t threads, size_t iteration, const RunResult& result) {
    _file << experiment << "," << operation << "," << mechanism << "," << std::fixed << std::setprecision(4)
          << violation_rate << "," << duplicate_rate << "," << size << "," << checkpoint_ops << "," << access_pattern << ","
          << threads << "," << iteration << "," << std::setprecision(3) << result.time_ms << "," << std::setprecision(2)
          << result.throughput_ops_per_s() << "\n";
    _file.flush();
  }

 private:
  std::ofstream _file;
};

// Configuration

struct Config {
  std::string experiment = "M0";
  std::vector<Mechanism> mechanisms;
  std::vector<Operation> operations;
  std::vector<double> violation_rates = {0.0, 0.1, 0.2, 1.0};
  std::vector<double> duplicate_rates = {0.1, 0.0};
  std::vector<size_t> sizes = {10000000};
  std::vector<AccessPattern> access_patterns = {AccessPattern::RANDOM, AccessPattern::SEQUENTIAL};
  std::vector<size_t> threads = {1, 2, 4, 8, 16, 32, 64};
  size_t iterations = 3;
  size_t checkpoint_interval = 0;  // 0 = one row per full run
  std::string output_file = "results.csv";
};

// Parsing helpers

std::vector<size_t> parse_size_list(const std::string& s) {
  std::vector<size_t> r;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ','))
    r.push_back(std::stoull(item));
  return r;
}

std::vector<double> parse_double_list(const std::string& s) {
  std::vector<double> r;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ','))
    r.push_back(std::stod(item));
  return r;
}

std::vector<Mechanism> parse_mechanisms(const std::string& s) {
  std::vector<Mechanism> r;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item == "off")
      r.push_back(Mechanism::OFF);
    else if (item == "fd_single")
      r.push_back(Mechanism::FD_SINGLE);
    else if (item == "fd_multi")
      r.push_back(Mechanism::FD_MULTI);
    else if (item == "od_single")
      r.push_back(Mechanism::OD_SINGLE);
    else if (item == "od_multi")
      r.push_back(Mechanism::OD_MULTI);
    else if (item == "all") {
      return {Mechanism::OFF, Mechanism::FD_SINGLE, Mechanism::FD_MULTI, Mechanism::OD_SINGLE, Mechanism::OD_MULTI};
    }
  }
  return r;
}

std::vector<Operation> parse_operations(const std::string& s) {
  std::vector<Operation> r;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item == "insert")
      r.push_back(Operation::INSERT);
    else if (item == "lookup")
      r.push_back(Operation::LOOKUP);
    else if (item == "delete")
      r.push_back(Operation::DELETE);
    else if (item == "all")
      return {Operation::INSERT, Operation::LOOKUP, Operation::DELETE};
  }
  return r;
}

// Main benchmark loop

void run_benchmarks(const Config& cfg) {
  CSVWriter csv(cfg.output_file);

  const size_t total_cells = cfg.mechanisms.size() * cfg.operations.size() * cfg.violation_rates.size() *
                             cfg.duplicate_rates.size() * cfg.sizes.size() * cfg.access_patterns.size() *
                             cfg.threads.size() * cfg.iterations;

  std::cout << "=== Micro-Benchmark: " << cfg.experiment << " ===" << std::endl;
  std::cout << "Mechanisms  : ";
  for (auto m : cfg.mechanisms)
    std::cout << mechanism_to_string(m) << " ";
  std::cout << "\nOperations  : ";
  for (auto o : cfg.operations)
    std::cout << operation_to_string(o) << " ";
  std::cout << "\nViol rates  : ";
  for (auto v : cfg.violation_rates)
    std::cout << (v * 100) << "% ";
  std::cout << "\nDup rates   : ";
  for (auto d : cfg.duplicate_rates)
    std::cout << (d * 100) << "% ";
  std::cout << "\nSizes       : ";
  for (auto s : cfg.sizes)
    std::cout << s << " ";
  std::cout << "\nCheckpoint  : "
            << (cfg.checkpoint_interval > 0 ? std::to_string(cfg.checkpoint_interval) : "disabled");
  std::cout << "\nThreads     : ";
  for (auto t : cfg.threads)
    std::cout << t << " ";
  std::cout << "\nIterations  : " << cfg.iterations;
  std::cout << "\nTotal cells : " << total_cells;
  std::cout << "\nOutput      : " << cfg.output_file << std::endl;
  std::cout << "===========================================" << std::endl;

  size_t cell = 0;

  for (auto pattern : cfg.access_patterns) {
    const std::string pat_str = pattern_to_string(pattern);

    for (size_t size : cfg.sizes) {
      for (double dup_rate : cfg.duplicate_rates) {
        // num_det: number of unique determinant keys
        // dup_rate=0.0 → all unique (num_det=size); dup_rate=0.9 → 10% unique
        const size_t num_det = std::max<size_t>(
            1, static_cast<size_t>(std::ceil(static_cast<double>(size) * (1.0 - dup_rate))));

        for (double viol_rate : cfg.violation_rates) {
          for (auto mech : cfg.mechanisms) {
            bool is_multi = (mech == Mechanism::FD_MULTI || mech == Mechanism::OD_MULTI);
            bool is_od = (mech == Mechanism::OD_SINGLE || mech == Mechanism::OD_MULTI);

            std::cout << "\n[MECH] === " << mechanism_to_string(mech) << " (multi=" << (is_multi ? "true" : "false")
                      << ", od=" << (is_od ? "true" : "false") << ") ===" << std::endl;

            // Generate data for THIS mechanism only (lazy, per-mechanism).
            // Only one set of entry vectors is alive at any time.
            const uint64_t seed = 42;
            std::vector<Entry> entries;
            std::vector<Entry> lookup_keys;

            if (is_od) {
              std::cout << "[DATA] Generating OD entries: " << pat_str << " size=" << size
                        << " viol=" << (viol_rate * 100) << "% multi=" << (is_multi ? "true" : "false") << " ..."
                        << std::flush;
              if (pattern == AccessPattern::RANDOM)
                entries = generate_od_entries(size, viol_rate, is_multi, seed);
              else
                entries = generate_od_entries_sequential(size, viol_rate, is_multi, seed);
            } else {
              // OFF, FD_SINGLE, FD_MULTI all use FD-style entries
              std::cout << "[DATA] Generating FD entries: " << pat_str << " size=" << size
                        << " viol=" << (viol_rate * 100) << "% dup=" << (dup_rate * 100) << "%"
                        << " multi=" << (is_multi ? "true" : "false") << " ..." << std::flush;
              if (pattern == AccessPattern::RANDOM)
                entries = generate_fd_entries(size, viol_rate, num_det, is_multi, seed);
              else
                entries = generate_fd_entries_sequential(size, viol_rate, num_det, is_multi, seed);
            }
            lookup_keys = generate_lookup_keys(entries, pattern, 123);
            std::cout << " done (" << (entries.size() * sizeof(Entry) / (1024 * 1024)) << " MB entries + "
                      << (lookup_keys.size() * sizeof(Entry) / (1024 * 1024)) << " MB keys)" << std::endl;

            for (auto op : cfg.operations) {
              for (size_t t : cfg.threads) {
                ++cell;
                std::cout << "  [" << cell << "/" << total_cells << "] " << mechanism_to_string(mech) << " / "
                          << operation_to_string(op) << " / " << pat_str << " / size=" << size
                          << " viol=" << (viol_rate * 100) << "%"
                          << " dup=" << (dup_rate * 100) << "%"
                          << " t=" << t << std::endl;

                for (size_t iter = 0; iter < cfg.iterations; ++iter) {
                  std::cout << "    iter " << (iter + 1) << "/" << cfg.iterations << " ..." << std::flush;
                  std::vector<RunResult> batch_results;
                  switch (op) {
                    case Operation::INSERT:
                      batch_results = run_insert(mech, entries, t, cfg.checkpoint_interval);
                      break;
                    case Operation::LOOKUP:
                      batch_results = run_lookup(mech, entries, lookup_keys, t, cfg.checkpoint_interval);
                      break;
                    case Operation::DELETE:
                      batch_results = run_delete(mech, entries, lookup_keys, t, cfg.checkpoint_interval);
                      break;
                  }

                  // Write one CSV row per batch result.
                  // checkpoint_ops = cumulative ops at end of this batch.
                  size_t cumulative_ops = 0;
                  double total_time_ms = 0.0;
                  for (const auto& r : batch_results) {
                    cumulative_ops += r.ops;
                    total_time_ms += r.time_ms;
                    csv.write(cfg.experiment, operation_to_string(op), mechanism_to_string(mech), viol_rate, dup_rate,
                              size, cumulative_ops, pat_str, t, iter, r);
                  }
                  const double total_tp =
                      (total_time_ms > 0.0) ? (static_cast<double>(cumulative_ops) / total_time_ms) * 1000.0 : 0.0;
                  std::cout << " " << std::fixed << std::setprecision(1) << total_time_ms << " ms ("
                            << std::setprecision(0) << (total_tp / 1e6) << "M ops/s)"
                            << (batch_results.size() > 1 ? " [" + std::to_string(batch_results.size()) + " batches]"
                                                         : "")
                            << std::endl;
                }
              }
            }
            // Vectors go out of scope here -- automatically freed before next mechanism
            std::cout << "[MEM] Freed entry vectors for " << mechanism_to_string(mech) << std::endl;
          }  // end mechanism loop
        }
      }
    }
  }

  std::cout << "\n=== Done. Results: " << cfg.output_file << " ===" << std::endl;
}

// CLI

void print_help(const char* prog) {
  std::cout << "Usage: " << prog << " [OPTIONS]\n\n"
            << "  --experiment        M0|M1                    (default: M0)\n"
            << "  --mechanism         off|fd_single|fd_multi|od_single|od_multi|all\n"
            << "                      M0 default: od_single,od_multi\n"
            << "                      M1 default: off,fd_single,fd_multi,od_single,od_multi\n"
            << "  --operation         insert|lookup|delete|all  (default: all)\n"
            << "  --violation-rates   r1,r2,...  (0.0-1.0)     (default: 0.0,0.1,0.2,1.0)\n"
            << "  --duplicate-rates   r1,r2,...  (0.0-1.0)     (default: 0.1,0.0)\n"
            << "  --sizes             s1,s2,...                 (default: 10000000)\n"
            << "  --checkpoint-interval N                       (record row every N ops; 0=disabled)\n"
            << "  --threads           t1,t2,...                 (default: 1,2,4,8,16,32,64)\n"
            << "  --access-pattern    random|sequential|both    (default: both)\n"
            << "  --iterations        N                         (default: 3)\n"
            << "  --output            FILE.csv                  (default: results.csv)\n"
            << "  --help\n";
}

int main(int argc, char* argv[]) {
  Config cfg;
  bool mechanism_set = false;
  bool operation_set = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      print_help(argv[0]);
      return 0;
    } else if (arg == "--experiment" && i + 1 < argc) {
      cfg.experiment = argv[++i];
    } else if (arg == "--mechanism" && i + 1 < argc) {
      cfg.mechanisms = parse_mechanisms(argv[++i]);
      mechanism_set = true;
    } else if (arg == "--operation" && i + 1 < argc) {
      cfg.operations = parse_operations(argv[++i]);
      operation_set = true;
    } else if (arg == "--violation-rates" && i + 1 < argc) {
      cfg.violation_rates = parse_double_list(argv[++i]);
    } else if (arg == "--duplicate-rates" && i + 1 < argc) {
      cfg.duplicate_rates = parse_double_list(argv[++i]);
    } else if (arg == "--sizes" && i + 1 < argc) {
      cfg.sizes = parse_size_list(argv[++i]);
    } else if (arg == "--checkpoint-interval" && i + 1 < argc) {
      cfg.checkpoint_interval = std::stoull(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      cfg.threads = parse_size_list(argv[++i]);
    } else if (arg == "--access-pattern" && i + 1 < argc) {
      std::string p = argv[++i];
      if (p == "random")
        cfg.access_patterns = {AccessPattern::RANDOM};
      else if (p == "sequential")
        cfg.access_patterns = {AccessPattern::SEQUENTIAL};
      else
        cfg.access_patterns = {AccessPattern::RANDOM, AccessPattern::SEQUENTIAL};
    } else if (arg == "--iterations" && i + 1 < argc) {
      cfg.iterations = std::stoull(argv[++i]);
    } else if (arg == "--output" && i + 1 < argc) {
      cfg.output_file = argv[++i];
    }
  }

  // Apply per-experiment defaults if not explicitly set.
  if (!mechanism_set) {
    if (cfg.experiment == "M1") {
      // Ablation: all 5 mechanisms
      cfg.mechanisms = {Mechanism::OFF, Mechanism::FD_SINGLE, Mechanism::FD_MULTI, Mechanism::OD_SINGLE,
                        Mechanism::OD_MULTI};
    } else {
      // M0: OD factorial
      cfg.mechanisms = {Mechanism::OD_SINGLE, Mechanism::OD_MULTI};
    }
  }
  if (!operation_set) {
    cfg.operations = {Operation::INSERT, Operation::LOOKUP, Operation::DELETE};
  }

  // M1 fixed-point defaults (only apply when not overridden)
  if (cfg.experiment == "M1") {
    if (cfg.violation_rates == std::vector<double>{0.0, 0.1, 0.2, 1.0})
      cfg.violation_rates = {0.0};
    if (cfg.duplicate_rates == std::vector<double>{0.1, 0.0})
      cfg.duplicate_rates = {0.1};
    if (cfg.threads == std::vector<size_t>{1, 2, 4, 8, 16, 32, 64})
      cfg.threads = {1, 16};
  }

  run_benchmarks(cfg);
  return 0;
}