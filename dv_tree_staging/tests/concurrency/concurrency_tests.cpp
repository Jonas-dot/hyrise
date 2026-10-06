#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "test_internal.hpp"

static int checks = 0;
static int failures = 0;

#define CHECK(condition)                                                                    \
    do {                                                                                    \
        ++checks;                                                                           \
        if (!(condition)) {                                                                 \
            ++failures;                                                                     \
            std::cerr << "FAIL " << __FILE__ << ':' << __LINE__ << ": " #condition "\n"; \
        }                                                                                   \
    } while (false)

static void test_history_readers_are_consistent() {
    constexpr int commits = 2000;
    constexpr int reader_count = 4;
    VersionedViolationHistory history(1);
    history.set_lowest_active(1); // force concurrent growth of the protected window
    std::atomic<bool> done{false};
    std::atomic<int> reader_errors{0};

    std::vector<std::thread> readers;
    for (int reader = 0; reader < reader_count; ++reader) {
        readers.emplace_back([&] {
            int64_t previous = 0;
            while (!done.load(std::memory_order_acquire)) {
                const int64_t current = history.query_latest();
                if (current < previous || current < 0 || current > commits) {
                    reader_errors.fetch_add(1, std::memory_order_relaxed);
                }
                previous = current;
            }
            if (history.query_latest() != commits) {
                reader_errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::thread writer([&] {
        for (int cid = 1; cid <= commits; ++cid) history.update(cid, 1);
        done.store(true, std::memory_order_release);
    });
    writer.join();
    for (auto& reader : readers) reader.join();

    CHECK(reader_errors.load() == 0);
    CHECK(history.query_latest() == commits);
    CHECK(history.size() == commits);
    CHECK(history.query_exact(1) == 1);
    history.set_lowest_active(commits);
    CHECK(history.size() == 1);
    CHECK(history.query_latest() == commits);
}

static void test_a1_torn_leaf_snapshot_forces_lookup_restart() {
    BTreeCppAdapter tree;
    int payload = 7;
    const std::string key = make_normalized_key("a1", 1);
    tree.insert_new(key, &payload);
    const uint64_t before =
        tree.restart_count(BTreeCppAdapter::RestartPoint::LookupLeaf);
    tree.force_restart_once(BTreeCppAdapter::RestartPoint::LookupLeaf);
    CHECK(tree.find(key) == &payload);
    CHECK(tree.restart_count(BTreeCppAdapter::RestartPoint::LookupLeaf) == before + 1);
}

static void test_a2_stale_sibling_snapshot_forces_neighbor_restart() {
    constexpr int keys_per_side = 900;
    BTreeCppAdapter tree;
    int left_anchor = -1;
    int right_anchor = -2;
    std::vector<int> payloads(keys_per_side * 2);
    for (int i = 0; i < keys_per_side; ++i) {
        payloads[i] = i;
        tree.insert_new(make_normalized_key("a2", i), &payloads[i]);
    }
    const std::string left_key = make_normalized_key("a2", 1000000);
    const std::string probe = make_normalized_key("a2", 1500000);
    const std::string right_key = make_normalized_key("a2", 2000000);
    tree.insert_new(left_key, &left_anchor);
    tree.insert_new(right_key, &right_anchor);
    for (int i = 0; i < keys_per_side; ++i) {
        payloads[keys_per_side + i] = keys_per_side + i;
        tree.insert_new(make_normalized_key("a2", 3000000 + i),
                        &payloads[keys_per_side + i]);
    }

    const uint64_t before =
        tree.restart_count(BTreeCppAdapter::RestartPoint::NeighborSiblings);
    tree.force_restart_once(BTreeCppAdapter::RestartPoint::NeighborSiblings);
    const auto adjacent = tree.neighbors(probe);
    CHECK(adjacent.predecessor == &left_anchor);
    CHECK(adjacent.successor == &right_anchor);
    CHECK(tree.restart_count(BTreeCppAdapter::RestartPoint::NeighborSiblings) == before + 1);
}

static void test_a4_insert_restart_counts_entry_once() {
    BTreeCppAdapter tree;
    int first_payload = 1;
    int second_payload = 2;
    tree.insert_new(make_normalized_key("a4", 1), &first_payload);
    const auto before_stats = tree.stats();
    const uint64_t before_restarts =
        tree.restart_count(BTreeCppAdapter::RestartPoint::InsertLeaf);
    tree.force_restart_once(BTreeCppAdapter::RestartPoint::InsertLeaf);
    const std::string inserted_key = make_normalized_key("a4", 2);
    tree.insert_new(inserted_key, &second_payload);
    CHECK(tree.find(inserted_key) == &second_payload);
    CHECK(tree.stats().entries == before_stats.entries + 1);
    CHECK(tree.restart_count(BTreeCppAdapter::RestartPoint::InsertLeaf) ==
          before_restarts + 1);
}

static void test_olc_concurrent_splits_and_lookups() {
    constexpr int writer_count = 6;
    constexpr int keys_per_writer = 700;
    constexpr int total = writer_count * keys_per_writer;
    BTreeCppAdapter tree;
    std::vector<int> payloads(total);
    std::vector<std::string> keys(total);
    std::vector<std::atomic<bool>> published(total);
    for (int i = 0; i < total; ++i) {
        payloads[i] = i;
        keys[i] = make_normalized_key("olc-structural-contention", i,
                                      std::string(static_cast<std::size_t>(i % 19), 'x'));
        published[i].store(false, std::memory_order_relaxed);
    }

    std::atomic<bool> writers_done{false};
    std::atomic<int> lookup_errors{0};
    std::atomic<int> sibling_errors{0};
    std::vector<std::thread> readers;
    for (int reader_id = 0; reader_id < 3; ++reader_id) {
        readers.emplace_back([&, reader_id] {
            std::minstd_rand generator(0xC011C + reader_id);
            while (!writers_done.load(std::memory_order_acquire)) {
                const int index = static_cast<int>(generator() % total);
                if (published[index].load(std::memory_order_acquire) &&
                    tree.find(keys[index]) != &payloads[index]) {
                    lookup_errors.fetch_add(1, std::memory_order_relaxed);
                }
                const std::string probe = make_normalized_key(
                    "olc-structural-contention", index, "never-inserted-probe");
                const auto neighbors = tree.neighbors(probe);
                auto valid_payload = [&](void* payload) {
                    if (payload == nullptr) return true;
                    const auto address = reinterpret_cast<std::uintptr_t>(payload);
                    const auto begin = reinterpret_cast<std::uintptr_t>(payloads.data());
                    const auto end = begin + payloads.size() * sizeof(payloads.front());
                    return address >= begin && address < end &&
                           (address - begin) % sizeof(payloads.front()) == 0;
                };
                if (!valid_payload(neighbors.predecessor) ||
                    !valid_payload(neighbors.successor)) {
                    sibling_errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::vector<std::thread> writers;
    for (int writer_id = 0; writer_id < writer_count; ++writer_id) {
        writers.emplace_back([&, writer_id] {
            // The modular permutation makes every writer hit all key ranges, forcing
            // concurrent splits at several levels instead of disjoint append-only leaves.
            for (int offset = 0; offset < keys_per_writer; ++offset) {
                const int logical = writer_id * keys_per_writer + offset;
                const int index = (logical * 1879) % total;
                tree.insert_new(keys[index], &payloads[index]);
                published[index].store(true, std::memory_order_release);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    writers_done.store(true, std::memory_order_release);
    for (auto& reader : readers) reader.join();

    CHECK(lookup_errors.load() == 0); // A1: no torn page/payload escaped validation
    CHECK(sibling_errors.load() == 0); // A2: no stale/torn cross-leaf payload escaped
    for (int i = 0; i < total; ++i) CHECK(tree.find(keys[i]) == &payloads[i]);
    const auto stats = tree.stats();
    CHECK(stats.entries == total); // A4: retries never double-counted or lost an insert
    CHECK(stats.height > 1);
    CHECK(stats.linked_leaf_nodes == stats.leaf_nodes);
    CHECK(stats.leaf_chain_valid); // A6: the complete three-leaf relink remained bidirectional
}

static void test_olc_recursive_inner_splits_with_concurrent_readers() {
    constexpr int writer_count = 8;
    constexpr int reader_count = 3;
    constexpr int total = 60000;
    BTreeCppAdapter tree;
    std::vector<int> payloads(total);
    std::vector<std::string> keys(total);
    std::vector<std::atomic<bool>> published(total);
    for (int i = 0; i < total; ++i) {
        payloads[i] = i;
        keys[i] = make_normalized_key(
            "olc-recursive-inner-split", i,
            std::string(static_cast<std::size_t>((i * 13) % 71),
                        static_cast<char>('a' + i % 26)));
        published[i].store(false, std::memory_order_relaxed);
    }

    std::atomic<bool> writers_done{false};
    std::atomic<int> lookup_errors{0};
    std::vector<std::thread> readers;
    for (int reader_id = 0; reader_id < reader_count; ++reader_id) {
        readers.emplace_back([&, reader_id] {
            std::minstd_rand generator(0x1A2B3u + reader_id);
            while (!writers_done.load(std::memory_order_acquire)) {
                const int index = static_cast<int>(generator() % total);
                if (published[index].load(std::memory_order_acquire) &&
                    tree.find(keys[index]) != &payloads[index]) {
                    lookup_errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::vector<std::thread> writers;
    for (int writer_id = 0; writer_id < writer_count; ++writer_id) {
        writers.emplace_back([&, writer_id] {
            for (int logical = writer_id; logical < total; logical += writer_count) {
                // 1879 is coprime with 60000, so this is a full permutation that
                // makes every writer contend across the entire key space.
                const int index = (logical * 1879) % total;
                tree.insert_new(keys[index], &payloads[index]);
                published[index].store(true, std::memory_order_release);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    writers_done.store(true, std::memory_order_release);
    for (auto& reader : readers) reader.join();

    int final_lookup_errors = 0;
    for (int i = 0; i < total; ++i) {
        if (tree.find(keys[i]) != &payloads[i]) ++final_lookup_errors;
    }
    const auto stats = tree.stats();
    CHECK(lookup_errors.load() == 0);
    CHECK(final_lookup_errors == 0);
    CHECK(stats.entries == total);
    CHECK(stats.height >= 3);
    CHECK(stats.inner_nodes > 1);
    CHECK(stats.linked_leaf_nodes == stats.leaf_nodes);
    CHECK(stats.leaf_chain_valid);
}

static void test_registered_fd_same_entry_waits_for_lower_cid() {
    DVTree index(DependencyKind::FD, 32);
    const std::string lhs = make_normalized_key("ticket-fd-same");
    const std::string a = make_normalized_key(10);
    const std::string b = make_normalized_key(20);

    Transaction seed;
    seed.insert(lhs, a);
    CHECK(index.apply_commit(seed) == 1);

    auto t1 = index.begin_commit();
    auto t2 = index.begin_commit();
    CHECK(t1.commit_id() == 2);
    CHECK(t2.commit_id() == 3);
    t1.insert(lhs, b);
    t2.remove(lhs, b);

    // CID 3 registers after CID 2 but cannot prepare this conflicting key until
    // CID 2 has installed it.
    t2.seal();
    CHECK(index.visible_commit_id() == 1);
    t1.seal();
    t1.wait_until_visible();
    t2.wait_until_visible();

    const auto state = index.snapshot_entry(lhs);
    CHECK(state.has_value());
    CHECK(state->rhs_counts.size() == 1);
    CHECK(state->rhs_counts.front().first == a);
    CHECK(index.violations_exact(CommitID{2}) == 1);
    CHECK(index.violations_exact(CommitID{3}) == 0);
    CHECK(index.visible_commit_id() == 3);
    CHECK(index.optimistic_reprepare_count_for_test() == 0);
}

static void test_registered_fd_disjoint_preparation_stays_valid() {
    DVTree index(DependencyKind::FD, 32);
    const std::string x = make_normalized_key("ticket-fd-x");
    const std::string y = make_normalized_key("ticket-fd-y");
    const std::string a = make_normalized_key(10);
    const std::string b = make_normalized_key(20);
    const std::string c = make_normalized_key(30);
    const std::string d = make_normalized_key(40);

    Transaction seed;
    seed.insert(x, a);
    seed.insert(y, c);
    CHECK(index.apply_commit(seed) == 1);

    auto t1 = index.begin_commit();
    auto t2 = index.begin_commit();
    t1.insert(x, b);
    t2.insert(y, d);
    t2.seal();
    const uint64_t before = index.optimistic_reprepare_count_for_test();
    t1.seal();
    t1.wait_until_visible();
    t2.wait_until_visible();

    CHECK(index.snapshot_entry(x)->rhs_counts.size() == 2);
    CHECK(index.snapshot_entry(y)->rhs_counts.size() == 2);
    CHECK(index.violations_exact(CommitID{2}) == 1);
    CHECK(index.violations_exact(CommitID{3}) == 2);
    CHECK(index.optimistic_reprepare_count_for_test() == before);
}

static void test_registered_noop_and_abort_close_cid_gaps() {
    DVTree index(DependencyKind::FD, 16);
    const std::string lhs = make_normalized_key("ticket-noop");
    const std::string rhs = make_normalized_key(1);

    auto aborted = index.begin_commit();
    auto writer = index.begin_commit();
    writer.insert(lhs, rhs);
    writer.seal();
    CHECK(index.visible_commit_id() == UNSET_COMMIT_ID);
    aborted.abort();
    aborted.wait_until_visible();
    writer.wait_until_visible();
    CHECK(index.visible_commit_id() == 2);
    CHECK(index.snapshot_entry(lhs)->rhs_counts.size() == 1);

    auto explicit_noop = index.begin_commit();
    explicit_noop.seal();
    explicit_noop.wait_until_visible();
    CHECK(index.visible_commit_id() == 3);
    CHECK(index.violations_exact(CommitID{1}) == 0);
    CHECK(index.violations_exact(CommitID{2}) == 0);
    CHECK(index.violations_exact(CommitID{3}) == 0);
}

static void test_registered_od_overlap_waits_for_complete_interval() {
    DVTree index(DependencyKind::OD, 32);
    const std::string a = make_normalized_key("ticket-od-a");
    const std::string c = make_normalized_key("ticket-od-c");
    const std::string ten = make_normalized_key(10);
    const std::string thirty = make_normalized_key(30);
    const std::string forty = make_normalized_key(40);
    const std::string fifty = make_normalized_key(50);

    Transaction seed;
    seed.insert(a, ten);
    seed.insert(c, thirty);
    CHECK(index.apply_commit(seed) == 1);

    auto t1 = index.begin_commit();
    auto t2 = index.begin_commit();
    t1.insert(a, forty); // local violation plus A->C boundary violation
    t2.insert(c, fifty); // overlaps T1 through the same OD boundary interval
    t2.seal();
    const uint64_t before = index.optimistic_reprepare_count_for_test();
    t1.seal();
    t1.wait_until_visible();
    t2.wait_until_visible();

    CHECK(index.violations_exact(CommitID{2}) == 2);
    CHECK(index.violations_exact(CommitID{3}) == 3);
    CHECK(index.optimistic_reprepare_count_for_test() == before);
    CHECK(index.snapshot_entry(a)->neighbor_violation == 1);
    CHECK(index.snapshot_entry(c)->local_violations == 1);
}

static void test_disjoint_higher_cid_installs_before_lower_failure_is_resolved() {
    DVTree index(DependencyKind::FD, 16);
    const std::string x = make_normalized_key("reservation-disjoint-x");
    const std::string y = make_normalized_key("reservation-disjoint-y");
    const std::string a = make_normalized_key(10);
    const std::string missing = make_normalized_key(20);
    const std::string c = make_normalized_key(30);
    const std::string d = make_normalized_key(40);

    Transaction seed;
    seed.insert(x, a);
    seed.insert(y, c);
    CHECK(index.apply_commit(seed) == 1);

    auto lower = index.begin_commit();
    auto higher = index.begin_commit();
    lower.remove(x, missing); // definitive error keeps CID 2 unresolved
    higher.insert(y, d);      // disjoint CID 3 may install before CID 2 retires
    lower.seal();
    higher.seal();

    CHECK(index.visible_commit_id() == 1);
    CHECK(index.effects_installed_for_test(CommitID{3}));
    CHECK(index.snapshot_entry(y)->rhs_counts.size() == 2);

    lower.abort();
    lower.wait_until_visible();
    higher.wait_until_visible();
    CHECK(index.visible_commit_id() == 3);
    CHECK(index.violations_exact(CommitID{2}) == 0);
    CHECK(index.violations_exact(CommitID{3}) == 1);
}

static void test_mixed_fd_footprint_prepares_free_key_while_conflict_waits() {
    DVTree index(DependencyKind::FD, 16);
    const std::string x = make_normalized_key("reservation-mixed-x");
    const std::string y = make_normalized_key("reservation-mixed-y");
    const std::string a = make_normalized_key(10);
    const std::string missing = make_normalized_key(20);
    const std::string b = make_normalized_key(30);
    const std::string c = make_normalized_key(40);
    const std::string d = make_normalized_key(50);

    Transaction seed;
    seed.insert(x, a);
    seed.insert(y, c);
    CHECK(index.apply_commit(seed) == 1);

    auto lower = index.begin_commit();
    auto higher = index.begin_commit();
    lower.remove(x, missing);
    higher.insert(x, b);
    higher.insert(y, d);
    lower.seal();
    higher.seal();

    // Y is prepared privately; X is still blocked by the unresolved lower CID.
    CHECK(index.prepared_key_count_for_test(CommitID{3}) == 1);
    CHECK(!index.effects_installed_for_test(CommitID{3}));
    CHECK(index.snapshot_entry(y)->rhs_counts.size() == 1);
    const auto waiting_memory = index.memory_statistics();
    CHECK(waiting_memory.active_commit_envelopes == 2);
    CHECK(waiting_memory.staged_operations == 3);
    CHECK(waiting_memory.prepared_entries >= 1);
    CHECK(waiting_memory.prepared_rhs_values >= 2);
    CHECK(waiting_memory.prepared_current_bytes > 0);
    CHECK(waiting_memory.prepared_peak_bytes >= waiting_memory.prepared_current_bytes);

    std::atomic<bool> higher_finished{false};
    std::thread waiter([&] {
        higher.wait_until_visible();
        higher_finished.store(true, std::memory_order_release);
    });
    while (index.reservation_wait_count_for_test() == 0) std::this_thread::yield();
    CHECK(!higher_finished.load(std::memory_order_acquire));
    CHECK(index.prepared_key_count_for_test(CommitID{3}) == 1);

    lower.abort();
    lower.wait_until_visible();
    waiter.join();
    CHECK(higher_finished.load(std::memory_order_acquire));
    CHECK(index.snapshot_entry(x)->rhs_counts.size() == 2);
    CHECK(index.snapshot_entry(y)->rhs_counts.size() == 2);
    CHECK(index.violations_exact(CommitID{3}) == 2);
    const auto completed_memory = index.memory_statistics();
    CHECK(completed_memory.active_commit_envelopes == 0);
    CHECK(completed_memory.prepared_current_bytes == 0);
    CHECK(completed_memory.prepared_peak_bytes >= waiting_memory.prepared_peak_bytes);
}

static void test_od_interval_reservation_waits_without_installing() {
    DVTree index(DependencyKind::OD, 16);
    const std::string a = make_normalized_key("reservation-od-a");
    const std::string c = make_normalized_key("reservation-od-c");
    const std::string ten = make_normalized_key(10);
    const std::string missing = make_normalized_key(20);
    const std::string thirty = make_normalized_key(30);
    const std::string forty = make_normalized_key(40);

    Transaction seed;
    seed.insert(a, ten);
    seed.insert(c, thirty);
    CHECK(index.apply_commit(seed) == 1);

    auto lower = index.begin_commit();
    auto higher = index.begin_commit();
    lower.remove(a, missing);
    higher.insert(c, forty);
    lower.seal();
    higher.seal();
    CHECK(!index.effects_installed_for_test(CommitID{3}));

    std::thread waiter([&] { higher.wait_until_visible(); });
    while (index.reservation_wait_count_for_test() == 0) std::this_thread::yield();
    CHECK(!index.effects_installed_for_test(CommitID{3}));

    lower.abort();
    lower.wait_until_visible();
    waiter.join();
    CHECK(index.snapshot_entry(c)->rhs_counts.size() == 2);
    CHECK(index.violations_exact(CommitID{3}) == 1);
}

static void test_registered_many_threads_reverse_completion_fd() {
    constexpr int key_count = 8;
    constexpr int commit_count = 64;
    DVTree index(DependencyKind::FD, 128);
    std::vector<std::string> lhs_keys(key_count);
    Transaction seed;
    for (int key = 0; key < key_count; ++key) {
        lhs_keys[key] = make_normalized_key("ticket-many-fd", key);
        seed.insert(lhs_keys[key], make_normalized_key("seed", key));
    }
    CHECK(index.apply_commit(seed) == 1);

    std::vector<DVTree::CommitTicket> tickets;
    tickets.reserve(commit_count);
    for (int commit = 0; commit < commit_count; ++commit) {
        auto ticket = index.begin_commit();
        ticket.insert(lhs_keys[commit % key_count],
                      make_normalized_key("rhs", commit));
        tickets.push_back(std::move(ticket));
    }

    std::vector<std::atomic<bool>> release(commit_count);
    for (auto& flag : release) flag.store(false, std::memory_order_relaxed);
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    threads.reserve(commit_count);
    for (int commit = 0; commit < commit_count; ++commit) {
        threads.emplace_back([&, commit, ticket = std::move(tickets[commit])]() mutable {
            while (!release[commit].load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            try {
                ticket.seal();
                ticket.wait_until_visible();
            } catch (...) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Higher CIDs are deliberately made ready first. None may become visible until
    // the final release makes the lowest outstanding CID ready.
    for (int commit = commit_count - 1; commit >= 0; --commit) {
        release[commit].store(true, std::memory_order_release);
        std::this_thread::yield();
    }
    for (auto& thread : threads) thread.join();

    CHECK(errors.load() == 0);
    CHECK(index.visible_commit_id() == CommitID{commit_count + 1});
    CHECK(index.violations_exact(CommitID{1}) == 0);
    CHECK(index.violations_exact(CommitID{commit_count + 1}) == commit_count);
    for (int key = 0; key < key_count; ++key) {
        CHECK(index.snapshot_entry(lhs_keys[key])->rhs_counts.size() == 9);
    }
}

static void test_registered_many_threads_reverse_completion_od() {
    constexpr int key_count = 16;
    DVTree index(DependencyKind::OD, 64);
    std::vector<std::string> lhs_keys(key_count);
    Transaction seed;
    for (int key = 0; key < key_count; ++key) {
        lhs_keys[key] = make_normalized_key("ticket-many-od", key);
        seed.insert(lhs_keys[key], make_normalized_key(key * 100));
    }
    CHECK(index.apply_commit(seed) == 1);

    std::vector<DVTree::CommitTicket> tickets;
    tickets.reserve(key_count);
    for (int key = 0; key < key_count; ++key) {
        auto ticket = index.begin_commit();
        ticket.insert(lhs_keys[key], make_normalized_key(key * 100 + 1));
        tickets.push_back(std::move(ticket));
    }

    std::vector<std::atomic<bool>> release(key_count);
    for (auto& flag : release) flag.store(false, std::memory_order_relaxed);
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    for (int key = 0; key < key_count; ++key) {
        threads.emplace_back([&, key, ticket = std::move(tickets[key])]() mutable {
            while (!release[key].load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            try {
                ticket.seal();
                ticket.wait_until_visible();
            } catch (...) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (int key = key_count - 1; key >= 0; --key) {
        release[key].store(true, std::memory_order_release);
        std::this_thread::yield();
    }
    for (auto& thread : threads) thread.join();

    CHECK(errors.load() == 0);
    CHECK(index.visible_commit_id() == CommitID{key_count + 1});
    CHECK(index.violations_exact(CommitID{key_count + 1}) == key_count);
    for (int key = 0; key < key_count; ++key) {
        const auto state = index.snapshot_entry(lhs_keys[key]);
        CHECK(state->local_violations == 1);
        CHECK(state->neighbor_violation == 0);
    }
}

static void test_olc_duplicate_insert_race() {
    constexpr int contenders = 12;
    BTreeCppAdapter tree;
    const std::string key = make_normalized_key("one-winner");
    std::vector<int> payloads(contenders);
    std::atomic<int> winners{0};
    std::atomic<int> duplicates{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < contenders; ++i) {
        payloads[i] = i;
        threads.emplace_back([&, i] {
            try {
                tree.insert_new(key, &payloads[i]);
                winners.fetch_add(1, std::memory_order_relaxed);
            } catch (const BTreeCppAdapter::DuplicateKey&) {
                duplicates.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CHECK(winners.load() == 1);
    CHECK(duplicates.load() == contenders - 1);
    CHECK(tree.find(key) != nullptr);
}

static void test_a6_three_leaf_relink_keeps_exact_neighbors() {
    constexpr int inserts_per_side = 1600;
    BTreeCppAdapter tree;
    int left_anchor = -1;
    int right_anchor = -2;
    const std::string left_key = make_normalized_key("stable-gap", 1000000);
    const std::string probe_key = make_normalized_key("stable-gap", 1500000);
    const std::string right_key = make_normalized_key("stable-gap", 2000000);
    tree.insert_new(left_key, &left_anchor);
    tree.insert_new(right_key, &right_anchor);

    std::vector<int> lower_payloads(inserts_per_side);
    std::vector<int> upper_payloads(inserts_per_side);
    std::atomic<bool> done{false};
    std::atomic<int> neighbor_errors{0};
    std::atomic<int> neighbor_reads{0};
    std::thread reader([&] {
        while (!done.load(std::memory_order_acquire)) {
            const auto adjacent = tree.neighbors(probe_key);
            neighbor_reads.fetch_add(1, std::memory_order_relaxed);
            if (adjacent.predecessor != &left_anchor || adjacent.successor != &right_anchor) {
                neighbor_errors.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::thread lower_writer([&] {
        for (int i = 0; i < inserts_per_side; ++i) {
            lower_payloads[i] = i;
            tree.insert_new(make_normalized_key("stable-gap", i), &lower_payloads[i]);
        }
    });
    std::thread upper_writer([&] {
        for (int i = 0; i < inserts_per_side; ++i) {
            upper_payloads[i] = i;
            tree.insert_new(make_normalized_key("stable-gap", 3000000 + i), &upper_payloads[i]);
        }
    });
    lower_writer.join();
    upper_writer.join();
    done.store(true, std::memory_order_release);
    reader.join();

    const auto adjacent = tree.neighbors(probe_key);
    CHECK(neighbor_reads.load() > 0);
    CHECK(neighbor_errors.load() == 0); // A2/A6: no stale cross-leaf link was accepted
    CHECK(adjacent.predecessor == &left_anchor);
    CHECK(adjacent.successor == &right_anchor);
    CHECK(tree.stats().leaf_chain_valid);
}

static void test_committed_reads_never_observe_partial_transaction() {
    constexpr int transactions = 250;
    DVTree index(DependencyKind::FD, transactions + 16);
    const std::string lhs = make_normalized_key("same-key", 1);
    Transaction seed;
    seed.insert(lhs, make_normalized_key(-1));
    CHECK(index.apply_commit(seed) == 1);

    std::atomic<bool> done{false};
    std::atomic<int> reader_errors{0};
    std::thread reader([&] {
        int64_t previous = 0;
        while (!done.load(std::memory_order_acquire)) {
            const int64_t committed = index.violations();
            if ((committed & 1) != 0 || committed < previous ||
                committed > transactions * 2) {
                reader_errors.fetch_add(1, std::memory_order_relaxed);
            }
            const auto memory = index.memory_statistics();
            if (memory.dependency_entries != 1 || memory.btree_leaf_pages == 0 ||
                memory.rhs_allocated_bytes < memory.rhs_live_entry_bytes ||
                memory.prepared_peak_bytes < memory.prepared_current_bytes ||
                memory.total_accounted_bytes == 0) {
                reader_errors.fetch_add(1, std::memory_order_relaxed);
            }
            previous = committed;
        }
        if (index.violations() != transactions * 2) {
            reader_errors.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::thread writer([&] {
        for (int i = 0; i < transactions; ++i) {
            Transaction transaction;
            transaction.insert(lhs, make_normalized_key(i * 2));
            transaction.insert(lhs, make_normalized_key(i * 2 + 1));
            index.apply_commit(transaction);
        }
        done.store(true, std::memory_order_release);
    });
    writer.join();
    reader.join();

    CHECK(reader_errors.load() == 0);
    CHECK(index.violations() == transactions * 2);
    CHECK(index.violations_live_uncommitted() == transactions * 2);
    const auto memory = index.memory_statistics();
    CHECK(memory.active_commit_envelopes == 0);
    CHECK(memory.prepared_current_bytes == 0);
    CHECK(memory.prepared_peak_bytes > 0);
}

static void test_parallel_fd_same_key_distinct_and_duplicate_values() {
    constexpr int thread_count = 4;
    constexpr int values_per_thread = 80;
    const std::string lhs = make_normalized_key("shared-hot-key");
    const std::string base_rhs = make_normalized_key(-1);

    DVTree distinct_index(DependencyKind::FD,
                                   thread_count * values_per_thread + 16);
    Transaction seed;
    seed.insert(lhs, base_rhs);
    distinct_index.apply_commit(seed);

    std::vector<std::thread> writers;
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            for (int i = 0; i < values_per_thread; ++i) {
                Transaction transaction;
                transaction.insert(lhs, make_normalized_key(thread_id * values_per_thread + i));
                distinct_index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();
    const int distinct_values = 1 + thread_count * values_per_thread;
    CHECK(distinct_index.find(lhs)->distinct_rhs() == distinct_values);
    CHECK(distinct_index.violations() == distinct_values - 1);

    // The same-key transaction latch makes one contender introduce the shared RHS;
    // the dedicated M3 test below also checks its exact intermediate CID attribution.
    DVTree duplicate_index(DependencyKind::FD, thread_count + 16);
    duplicate_index.apply_commit(seed);
    const std::string shared_rhs = make_normalized_key(999);
    writers.clear();
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&] {
            Transaction transaction;
            transaction.insert(lhs, shared_rhs);
            duplicate_index.apply_commit(transaction);
        });
    }
    for (auto& writer : writers) writer.join();
    DependencyEntry* entry = duplicate_index.find(lhs);
    CHECK(entry->distinct_rhs() == 2);
    CHECK(entry->rhs_counts.find(std::string_view(shared_rhs))->second == thread_count);
    CHECK(duplicate_index.violations() == 1);
}

static void test_a5_remove_reinsert_is_commit_atomic() {
    // A5 is architecturally removed from the tree: its payload is a stable pointer and
    // never undergoes Hyrise's remove/reinsert resize path. At the DV layer, this proves
    // the public committed verdict cannot observe the transaction's internal pair.
    constexpr int updates = 500;
    DVTree index(DependencyKind::FD, 16);
    const std::string lhs = make_normalized_key("update-atomicity");
    const std::string ten = make_normalized_key(10);
    const std::string twenty = make_normalized_key(20);
    Transaction seed;
    seed.insert(lhs, ten);
    index.apply_commit(seed);

    std::atomic<bool> done{false};
    std::atomic<int> dirty_reads{0};
    std::thread reader([&] {
        while (!done.load(std::memory_order_acquire)) {
            if (!index.holds() || index.violations() != 0) {
                dirty_reads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    std::thread writer([&] {
        bool currently_ten = true;
        for (int i = 0; i < updates; ++i) {
            Transaction transaction;
            if (currently_ten) transaction.update(lhs, ten, twenty);
            else transaction.update(lhs, twenty, ten);
            index.apply_commit(transaction);
            currently_ten = !currently_ten;
        }
        done.store(true, std::memory_order_release);
    });
    writer.join();
    reader.join();
    CHECK(dirty_reads.load() == 0);
    CHECK(index.holds());
    CHECK(index.find(lhs)->distinct_rhs() == 1);
}

static void test_parallel_fd_writers_on_distinct_keys() {
    constexpr int thread_count = 4;
    constexpr int values_per_key = 100;
    DVTree index(DependencyKind::FD, thread_count * values_per_key + 16);
    std::vector<std::thread> writers;

    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            const std::string lhs = make_normalized_key("thread", thread_id);
            for (int value = 0; value < values_per_key; ++value) {
                Transaction transaction;
                transaction.insert(lhs, make_normalized_key(value));
                index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();

    const int64_t expected = thread_count * (values_per_key - 1);
    CHECK(index.violations() == expected);
    CHECK(index.violations_live_uncommitted() == expected);
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        DependencyEntry* entry = index.find(make_normalized_key("thread", thread_id));
        CHECK(entry != nullptr);
        CHECK(entry->distinct_rhs() == values_per_key);
        CHECK(entry->local_violations == values_per_key - 1);
    }
}

static void test_a3_cross_leaf_od_boundary_matches_batch_recompute() {
    // With enough keys to span many engine leaves, N descending RHS values create N-1
    // boundaries, including every cross-leaf boundary (A3).
    constexpr int thread_count = 4;
    constexpr int keys_per_thread = 80;
    DVTree index(DependencyKind::OD, thread_count * keys_per_thread + 16);
    std::vector<std::thread> writers;

    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            for (int i = 0; i < keys_per_thread; ++i) {
                const int key = thread_id + i * thread_count;
                Transaction transaction;
                transaction.insert(make_normalized_key(key),
                                   make_normalized_key(10000 - key));
                index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();

    const int64_t incremental = index.violations();
    CHECK(incremental == thread_count * keys_per_thread - 1);
    CHECK(index.violations_live_uncommitted() == incremental);
    index.compute_verdict();
    CHECK(index.violations_live_uncommitted() == incremental);
}

static void test_m3_same_value_delta_has_exact_commit_attribution() {
    DVTree index(DependencyKind::FD, 32);
    const std::string lhs = make_normalized_key("m3-exact");
    const std::string base = make_normalized_key(10);
    const std::string shared = make_normalized_key(20);
    Transaction seed;
    seed.insert(lhs, base);
    CHECK(index.apply_commit(seed) == 1);

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    CommitID insert_cids[2]{};
    std::thread first([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        Transaction transaction;
        transaction.insert(lhs, shared);
        insert_cids[0] = index.apply_commit(transaction);
    });
    std::thread second([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        Transaction transaction;
        transaction.insert(lhs, shared);
        insert_cids[1] = index.apply_commit(transaction);
    });
    while (ready.load(std::memory_order_acquire) != 2) std::this_thread::yield();
    start.store(true, std::memory_order_release);
    first.join();
    second.join();
    std::sort(std::begin(insert_cids), std::end(insert_cids));

    CHECK(insert_cids[0] == 2);
    CHECK(insert_cids[1] == 3);
    CHECK(index.violations_exact(insert_cids[0]) == 1);
    CHECK(index.violations_exact(insert_cids[1]) == 1);
    const auto after_insert = index.snapshot_entry(lhs);
    CHECK(after_insert.has_value());
    CHECK(after_insert->rhs_counts.size() == 2);
    CHECK(after_insert->rhs_counts[1].second == 2);

    ready.store(0, std::memory_order_relaxed);
    start.store(false, std::memory_order_relaxed);
    CommitID remove_cids[2]{};
    std::thread third([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        Transaction transaction;
        transaction.remove(lhs, shared);
        remove_cids[0] = index.apply_commit(transaction);
    });
    std::thread fourth([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        Transaction transaction;
        transaction.remove(lhs, shared);
        remove_cids[1] = index.apply_commit(transaction);
    });
    while (ready.load(std::memory_order_acquire) != 2) std::this_thread::yield();
    start.store(true, std::memory_order_release);
    third.join();
    fourth.join();
    std::sort(std::begin(remove_cids), std::end(remove_cids));

    CHECK(remove_cids[0] == 4);
    CHECK(remove_cids[1] == 5);
    CHECK(index.violations_exact(remove_cids[0]) == 1);
    CHECK(index.violations_exact(remove_cids[1]) == 0);
}

static void test_concurrent_fd_snapshots_equal_every_committed_prefix() {
    constexpr int thread_count = 4;
    constexpr int values_per_thread = 50;
    constexpr int total = thread_count * values_per_thread;
    DVTree index(DependencyKind::FD, total + 8);
    const std::string lhs = make_normalized_key("snapshot-prefix");
    std::vector<std::thread> writers;
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            for (int i = 0; i < values_per_thread; ++i) {
                Transaction transaction;
                transaction.insert(lhs,
                                   make_normalized_key(thread_id * values_per_thread + i));
                index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();

    CHECK(index.visible_commit_id() == total);
    for (CommitID cid{1}; cid <= total; ++cid) {
        CHECK(index.violations_exact(cid) == static_cast<int64_t>(cid - 1));
    }
}

static void test_multikey_fd_transactions_are_atomic_and_deadlock_free() {
    constexpr int thread_count = 4;
    constexpr int transactions_per_thread = 40;
    constexpr int total_transactions = thread_count * transactions_per_thread;
    DVTree index(DependencyKind::FD, total_transactions + 16);
    const std::string left = make_normalized_key("multi-key", 1);
    const std::string right = make_normalized_key("multi-key", 2);
    Transaction seed;
    seed.insert(left, make_normalized_key(-1));
    seed.insert(right, make_normalized_key(-1));
    index.apply_commit(seed);

    std::vector<std::thread> writers;
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            for (int i = 0; i < transactions_per_thread; ++i) {
                const int value = thread_id * transactions_per_thread + i;
                Transaction transaction;
                // Deliberately reverse call-site order for half the writers. The
                // implementation must still lock normalized LHS keys canonically.
                if ((thread_id & 1) == 0) {
                    transaction.insert(left, make_normalized_key(value));
                    transaction.insert(right, make_normalized_key(value));
                } else {
                    transaction.insert(right, make_normalized_key(value));
                    transaction.insert(left, make_normalized_key(value));
                }
                index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();

    CHECK(index.visible_commit_id() == total_transactions + 1);
    for (CommitID cid{1}; cid <= total_transactions + 1; ++cid) {
        CHECK(index.violations_exact(cid) == static_cast<int64_t>((cid - 1) * 2));
    }
    CHECK(index.snapshot_entry(left)->rhs_counts.size() == total_transactions + 1);
    CHECK(index.snapshot_entry(right)->rhs_counts.size() == total_transactions + 1);
}

static void test_m3_od_duplicate_race_has_exact_prefix() {
    DVTree index(DependencyKind::OD, 16);
    const std::string lhs = make_normalized_key("od-m3");
    const std::string first_rhs = make_normalized_key(10);
    const std::string shared_rhs = make_normalized_key(20);
    Transaction seed;
    seed.insert(lhs, first_rhs);
    index.apply_commit(seed);

    CommitID cids[2]{};
    std::thread first([&] {
        Transaction transaction;
        transaction.insert(lhs, shared_rhs);
        cids[0] = index.apply_commit(transaction);
    });
    std::thread second([&] {
        Transaction transaction;
        transaction.insert(lhs, shared_rhs);
        cids[1] = index.apply_commit(transaction);
    });
    first.join();
    second.join();
    std::sort(cids, cids + 2);
    CHECK(cids[0] == 2);
    CHECK(cids[1] == 3);
    CHECK(index.violations_exact(CommitID{2}) == 1);
    CHECK(index.violations_exact(CommitID{3}) == 1);
    CHECK(index.snapshot_entry(lhs)->rhs_counts[1].second == 2);
}

static void test_thread_safe_entry_snapshots_preserve_metadata_invariants() {
    for (const DependencyKind kind : {DependencyKind::FD, DependencyKind::OD}) {
        constexpr int commits = 180;
        DVTree index(kind, commits + 16);
        const std::string lhs = make_normalized_key("entry-snapshot", kind == DependencyKind::FD ? 1 : 2);
        Transaction seed;
        seed.insert(lhs, make_normalized_key(-1));
        index.apply_commit(seed);

        std::atomic<bool> done{false};
        std::atomic<int> errors{0};
        std::thread reader([&] {
            while (!done.load(std::memory_order_acquire)) {
                const auto snapshot = index.snapshot_entry(lhs);
                if (!snapshot || snapshot->rhs_counts.empty() ||
                    snapshot->local_violations + 1 != snapshot->rhs_counts.size() ||
                    snapshot->version == UNSET_COMMIT_ID) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
        std::thread writer([&] {
            for (int value = 0; value < commits; ++value) {
                Transaction transaction;
                transaction.insert(lhs, make_normalized_key(value));
                index.apply_commit(transaction);
            }
            done.store(true, std::memory_order_release);
        });
        writer.join();
        reader.join();
        CHECK(errors.load() == 0);
        CHECK(index.snapshot_entry(lhs)->rhs_counts.size() == commits + 1);
    }
}

static void test_concurrent_od_tombstone_run() {
    constexpr int thread_count = 4;
    constexpr int key_count = 180;
    DVTree index(DependencyKind::OD, key_count * 2 + 16);
    Transaction seed;
    for (int key = 0; key < key_count; ++key) {
        seed.insert(make_normalized_key(key), make_normalized_key(key));
    }
    index.apply_commit(seed);
    CHECK(index.holds());

    std::vector<std::thread> writers;
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        writers.emplace_back([&, thread_id] {
            for (int key = thread_id; key < key_count; key += thread_count) {
                Transaction transaction;
                transaction.remove(make_normalized_key(key), make_normalized_key(key));
                index.apply_commit(transaction);
            }
        });
    }
    for (auto& writer : writers) writer.join();

    CHECK(index.holds());
    CHECK(index.violations() == 0);
    int count = 0;
    for (DependencyEntry* entry = index.first_entry(); entry != nullptr; entry = entry->right) {
        CHECK(entry->distinct_rhs() == 0);
        CHECK(entry->local_violations == 0);
        CHECK(entry->neighbor_violation == 0);
        ++count;
    }
    CHECK(count == key_count);
}

static void test_nonoverlapping_od_intervals_execute_concurrently() {
    constexpr int thread_count = 4;
    constexpr int initial_rhs_per_target = 160;
    constexpr int transactions_per_thread = 80;
    DVTree index(DependencyKind::OD,
                          thread_count * transactions_per_thread + 32);
    Transaction seed;
    for (int region = 0; region < thread_count; ++region) {
        const int base = region * 10000;
        seed.insert(make_normalized_key(base), make_normalized_key(base));
        for (int rhs = 0; rhs < initial_rhs_per_target; ++rhs) {
            seed.insert(make_normalized_key(base + 1),
                        make_normalized_key(base + 100 + rhs));
        }
        seed.insert(make_normalized_key(base + 2), make_normalized_key(base + 1000));
    }
    index.apply_commit(seed);

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> writers;
    for (int region = 0; region < thread_count; ++region) {
        writers.emplace_back([&, region] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            const int base = region * 10000;
            for (int i = 0; i < transactions_per_thread; ++i) {
                Transaction transaction;
                transaction.insert(make_normalized_key(base + 1),
                                   make_normalized_key(base + 10000 + i));
                index.apply_commit(transaction);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != thread_count) std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto& writer : writers) writer.join();

    CHECK(index.max_concurrent_od_transactions_for_test() > 1);
    const int64_t incremental = index.violations();
    index.compute_verdict();
    CHECK(index.violations_live_uncommitted() == incremental);
}

// Registered OD intervals are a snapshot of the non-empty pattern at seal time.
// Here V tombstones the two keys between p and s, so after V installs, p and s
// are adjacent and share the neighbour term neighbour(p). V is sealed last, so
// all three footprints are built on the pre-V chain: L (touching p) registers
// [p, a] and H (touching s) registers [b, s], which do not overlap. H therefore
// never waits for L even though both now recompute neighbour(p).
static void test_od_stale_disjoint_intervals_deterministic_prefix() {
    const std::string p = make_normalized_key(1);
    const std::string a = make_normalized_key(2);
    const std::string b = make_normalized_key(3);
    const std::string s = make_normalized_key(4);
    const std::string nine = make_normalized_key(9);
    const std::string five = make_normalized_key(5);
    const std::string three = make_normalized_key(3);
    const std::string absent = make_normalized_key(777);

    DVTree index(DependencyKind::OD, 64);
    Transaction seed;
    seed.insert(p, nine);
    seed.insert(a, nine);
    seed.insert(b, nine);
    seed.insert(s, five);
    CHECK(index.apply_commit(seed) == CommitID{1});

    auto tombstoner = index.begin_commit();  // cid 2: removes a and b
    auto stalled = index.begin_commit();     // cid 3: definitive error, blocks cid 4 only
    auto lower = index.begin_commit();       // cid 4: updates p
    auto higher = index.begin_commit();      // cid 5: inserts into s
    tombstoner.remove(a, nine);
    tombstoner.remove(b, nine);
    stalled.remove(p, absent);
    lower.update(p, nine, five);
    higher.insert(s, three);

    std::atomic<int> sealed{0};
    auto seal_and_wait = [&](DVTree::CommitTicket& ticket) {
        ticket.seal();
        sealed.fetch_add(1, std::memory_order_acq_rel);
        while (sealed.load(std::memory_order_acquire) < 3) std::this_thread::yield();
        ticket.wait_until_visible();
    };
    std::thread lower_thread([&] { seal_and_wait(lower); });
    std::thread higher_thread([&] { seal_and_wait(higher); });
    while (sealed.load(std::memory_order_acquire) < 2) std::this_thread::yield();
    stalled.seal();
    sealed.fetch_add(1, std::memory_order_acq_rel);

    // Sealing cid 2 last registers all four footprints against the pre-tombstone
    // chain, then drives them: cid 2 installs, cid 3 fails, cid 4 is skipped
    // because cid 3 still holds an overlapping reservation, and cid 5 installs
    // out of order because its registered interval [b, s] misses cid 4's [p, a].
    // Sealing cid 2 last drives all four inline: cid 2 installs, cid 3 fails,
    // and cid 4 is skipped because cid 3 still holds an overlapping reservation.
    // Ordering by the registered intervals alone let cid 5 install right here,
    // because its [b, s] misses cid 4's [p, a]; ordering by the window cid 5
    // actually prepares makes it wait, since that window reaches back to p.
    tombstoner.seal();
    CHECK(!index.effects_installed_for_test(CommitID{4}));

    stalled.abort();  // releases cid 4, which cid 5 must not have preceded
    tombstoner.wait_until_visible();
    lower_thread.join();
    higher_thread.join();

    CHECK(index.visible_commit_id() == CommitID{5});
    CHECK(index.violations_exact(CommitID{2}) == 1);
    // cid 4 applied on top of cid 2 alone: p={5}, s={5} -> no inversion at all.
    CHECK(index.violations_exact(CommitID{4}) == 0);
    CHECK(index.holds_exact(CommitID{4}));
    CHECK(index.violations_exact(CommitID{5}) == 2);
}

static void test_od_stale_disjoint_intervals_keep_prefix_exact() {
    const std::string p = make_normalized_key(1);
    const std::string a = make_normalized_key(2);
    const std::string b = make_normalized_key(3);
    const std::string s = make_normalized_key(4);
    const std::string nine = make_normalized_key(9);
    const std::string five = make_normalized_key(5);
    const std::string three = make_normalized_key(3);

    for (int iteration = 0; iteration < 300; ++iteration) {
        DVTree index(DependencyKind::OD, 64);
        Transaction seed;
        seed.insert(p, nine);
        seed.insert(a, nine);
        seed.insert(b, nine);
        seed.insert(s, five);
        CHECK(index.apply_commit(seed) == CommitID{1});

        auto tombstoner = index.begin_commit();  // cid 2
        auto lower = index.begin_commit();       // cid 3
        auto higher = index.begin_commit();      // cid 4
        tombstoner.remove(a, nine);
        tombstoner.remove(b, nine);
        lower.update(p, nine, five);
        higher.insert(s, three);

        std::atomic<int> sealed{0};
        auto seal_then_wait = [&](DVTree::CommitTicket& ticket) {
            ticket.seal();
            sealed.fetch_add(1, std::memory_order_acq_rel);
            while (sealed.load(std::memory_order_acquire) < 3) std::this_thread::yield();
            ticket.wait_until_visible();
        };
        std::thread lower_thread([&] { seal_then_wait(lower); });
        std::thread higher_thread([&] { seal_then_wait(higher); });
        // V seals last so every footprint is registered against the pre-V chain.
        while (sealed.load(std::memory_order_acquire) < 2) std::this_thread::yield();
        seal_then_wait(tombstoner);
        lower_thread.join();
        higher_thread.join();

        // Sequential truth in CID order:
        //   cid 2: p={9}, s={5}                 -> neighbour(p) = 9 > 5  = 1
        //   cid 3: p={5}, s={5}                 -> neighbour(p) = 5 > 5  = 0
        //   cid 4: p={5}, s={3,5}               -> local(s)=1, 5 > 3     = 2
        CHECK(index.violations_exact(CommitID{2}) == 1);
        CHECK(index.violations_exact(CommitID{3}) == 0);
        CHECK(index.holds_exact(CommitID{3}));
        CHECK(index.violations_exact(CommitID{4}) == 2);
    }
}

// Per-CID exactness under OD concurrency. The captured-replay chaos test below
// only compares the final state; this one compares violations_exact(cid) for
// every committed prefix against a sequential replay of the captured CID order.
// A small key domain with tombstone runs makes neighbour terms of different
// transactions depend on each other, which is where delta attribution can drift
// from installation order.
static void test_concurrent_od_every_prefix_matches_captured_replay() {
    constexpr int thread_count = 4;
    constexpr int key_count = 12;
    constexpr int rounds = 6;
    constexpr int total_operations = key_count + rounds * key_count;

    struct Operation {
        int key = 0;
        int value = 0;
        bool is_delete = false;
    };

    for (unsigned seed : {0x0D01u, 0x0D02u, 0x0D03u}) {
        std::vector<Operation> by_commit(total_operations + 1);
        std::atomic<int> next_commit_slot{0};
        DVTree concurrent(DependencyKind::OD, total_operations * 2 + 16);

        // Seed one row per key so every key starts non-empty and adjacent.
        std::mt19937 seed_generator(seed);
        std::vector<int> live_value(key_count);
        for (int key = 0; key < key_count; ++key) {
            live_value[key] = static_cast<int>(seed_generator() % 20);
            Transaction transaction;
            transaction.insert(make_normalized_key(key), make_normalized_key(live_value[key]));
            const CommitID cid = concurrent.apply_commit(transaction);
            by_commit[cid] = {key, live_value[key], false};
            next_commit_slot.fetch_add(1, std::memory_order_relaxed);
        }

        // Each thread owns a disjoint key partition so its deletes stay valid,
        // while all partitions interleave in one shared entry chain.
        std::vector<std::thread> writers;
        for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
            writers.emplace_back([&, thread_id] {
                std::mt19937 generator(seed + 0x9E37u * static_cast<unsigned>(thread_id + 1));
                std::vector<int> owned;
                for (int key = thread_id; key < key_count; key += thread_count) owned.push_back(key);
                std::vector<int> current(owned.size());
                for (std::size_t i = 0; i < owned.size(); ++i) current[i] = live_value[owned[i]];
                std::vector<bool> present(owned.size(), true);

                for (int round = 0; round < rounds; ++round) {
                    for (std::size_t i = 0; i < owned.size(); ++i) {
                        Transaction transaction;
                        Operation operation;
                        operation.key = owned[i];
                        if (present[i] && (generator() % 2) == 0) {
                            // Tombstone this key: its group becomes empty, so the
                            // neighbour terms of surrounding keys must skip it.
                            operation.value = current[i];
                            operation.is_delete = true;
                            transaction.remove(make_normalized_key(operation.key),
                                               make_normalized_key(operation.value));
                            present[i] = false;
                        } else {
                            operation.value = static_cast<int>(generator() % 20);
                            operation.is_delete = false;
                            transaction.insert(make_normalized_key(operation.key),
                                               make_normalized_key(operation.value));
                            current[i] = operation.value;
                            present[i] = true;
                        }
                        const CommitID cid = concurrent.apply_commit(transaction);
                        by_commit[cid] = operation;
                    }
                }
            });
        }
        for (auto& writer : writers) writer.join();

        DVTree replay(DependencyKind::OD, total_operations * 2 + 16);
        for (CommitID cid{1}; cid <= total_operations; ++cid) {
            const Operation& operation = by_commit[cid];
            Transaction transaction;
            if (operation.is_delete) {
                transaction.remove(make_normalized_key(operation.key), make_normalized_key(operation.value));
            } else {
                transaction.insert(make_normalized_key(operation.key), make_normalized_key(operation.value));
            }
            CHECK(replay.apply_commit(transaction) == cid);
            CHECK(concurrent.violations_exact(cid) == replay.violations_exact(cid));
            CHECK(concurrent.holds_exact(cid) == replay.holds_exact(cid));
        }
        CHECK(concurrent.violations() == replay.violations());
    }
}

static void test_concurrent_od_chaos_matches_captured_commit_replay() {
    constexpr int thread_count = 4;
    constexpr int key_count = 150;
    constexpr int delete_start = 50;
    constexpr int delete_count = 40;
    constexpr int total_operations = key_count + delete_count;

    std::vector<int> values(key_count);
    for (int i = 0; i < key_count; ++i) values[i] = i;
    std::mt19937 generator(12345);
    std::shuffle(values.begin(), values.end(), generator);

    struct Operation {
        int key = 0;
        int value = 0;
        bool is_delete = false;
    };
    std::vector<Operation> by_commit(total_operations + 1);
    DVTree concurrent(DependencyKind::OD, total_operations * 2 + 16);

    {
        std::vector<std::thread> writers;
        for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
            writers.emplace_back([&, thread_id] {
                for (int key = thread_id; key < key_count; key += thread_count) {
                    Transaction transaction;
                    transaction.insert(make_normalized_key(key), make_normalized_key(values[key]));
                    const CommitID cid = concurrent.apply_commit(transaction);
                    by_commit[cid] = {key, values[key], false};
                }
            });
        }
        for (auto& writer : writers) writer.join();
    }
    {
        std::vector<std::thread> writers;
        for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
            writers.emplace_back([&, thread_id] {
                for (int key = delete_start + thread_id;
                     key < delete_start + delete_count; key += thread_count) {
                    Transaction transaction;
                    transaction.remove(make_normalized_key(key), make_normalized_key(values[key]));
                    const CommitID cid = concurrent.apply_commit(transaction);
                    by_commit[cid] = {key, values[key], true};
                }
            });
        }
        for (auto& writer : writers) writer.join();
    }

    DVTree replay(DependencyKind::OD, total_operations * 2 + 16);
    for (CommitID cid{1}; cid <= total_operations; ++cid) {
        const Operation& operation = by_commit[cid];
        Transaction transaction;
        if (operation.is_delete) {
            transaction.remove(make_normalized_key(operation.key),
                               make_normalized_key(operation.value));
        } else {
            transaction.insert(make_normalized_key(operation.key),
                               make_normalized_key(operation.value));
        }
        CHECK(replay.apply_commit(transaction) == cid);
    }

    CHECK(concurrent.violations() == replay.violations());
    CHECK(concurrent.holds() == replay.holds());
    DependencyEntry* left = concurrent.first_entry();
    DependencyEntry* right = replay.first_entry();
    int compared = 0;
    bool equal = true;
    while (left != nullptr && right != nullptr) {
        if (left->lhs != right->lhs || left->distinct_rhs() != right->distinct_rhs() ||
            left->local_violations != right->local_violations ||
            left->neighbor_violation != right->neighbor_violation) {
            equal = false;
        }
        auto left_rhs = left->rhs_counts.begin();
        auto right_rhs = right->rhs_counts.begin();
        while (left_rhs != left->rhs_counts.end() && right_rhs != right->rhs_counts.end()) {
            if (DVTree::rhs_bytes(left_rhs->first) !=
                    DVTree::rhs_bytes(right_rhs->first) ||
                left_rhs->second != right_rhs->second) {
                equal = false;
            }
            ++left_rhs;
            ++right_rhs;
        }
        if (left_rhs != left->rhs_counts.end() || right_rhs != right->rhs_counts.end()) equal = false;
        left = left->right;
        right = right->right;
        ++compared;
    }
    CHECK(left == nullptr && right == nullptr);
    CHECK(compared == key_count);
    CHECK(equal);
}

int main() {
    test_history_readers_are_consistent();
    test_a1_torn_leaf_snapshot_forces_lookup_restart();
    test_a2_stale_sibling_snapshot_forces_neighbor_restart();
    test_a4_insert_restart_counts_entry_once();
    test_olc_concurrent_splits_and_lookups();
    test_olc_recursive_inner_splits_with_concurrent_readers();
    test_registered_fd_same_entry_waits_for_lower_cid();
    test_registered_fd_disjoint_preparation_stays_valid();
    test_registered_noop_and_abort_close_cid_gaps();
    test_registered_od_overlap_waits_for_complete_interval();
    test_disjoint_higher_cid_installs_before_lower_failure_is_resolved();
    test_mixed_fd_footprint_prepares_free_key_while_conflict_waits();
    test_od_interval_reservation_waits_without_installing();
    test_registered_many_threads_reverse_completion_fd();
    test_registered_many_threads_reverse_completion_od();
    test_olc_duplicate_insert_race();
    test_a6_three_leaf_relink_keeps_exact_neighbors();
    test_committed_reads_never_observe_partial_transaction();
    test_parallel_fd_same_key_distinct_and_duplicate_values();
    test_a5_remove_reinsert_is_commit_atomic();
    test_parallel_fd_writers_on_distinct_keys();
    test_a3_cross_leaf_od_boundary_matches_batch_recompute();
    test_m3_same_value_delta_has_exact_commit_attribution();
    test_concurrent_fd_snapshots_equal_every_committed_prefix();
    test_multikey_fd_transactions_are_atomic_and_deadlock_free();
    test_m3_od_duplicate_race_has_exact_prefix();
    test_thread_safe_entry_snapshots_preserve_metadata_invariants();
    test_concurrent_od_tombstone_run();
    test_nonoverlapping_od_intervals_execute_concurrently();
    test_od_stale_disjoint_intervals_deterministic_prefix();
    test_od_stale_disjoint_intervals_keep_prefix_exact();
    test_concurrent_od_every_prefix_matches_captured_replay();
    test_concurrent_od_chaos_matches_captured_commit_replay();

    if (failures != 0) {
        std::cerr << failures << " of " << checks << " checks failed\n";
        return 1;
    }
    std::cout << "all " << checks << " threaded checks passed\n";
    return 0;
}
