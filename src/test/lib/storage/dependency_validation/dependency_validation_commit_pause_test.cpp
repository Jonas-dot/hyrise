#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dependency_validation/dependency_validation_bootstrap.hpp"
#include "storage/dependency_validation/dependency_validation_test_hooks.hpp"
#include "storage/table.hpp"

namespace hyrise::dv_tree {

// Phase 8/14 adversarial scheduling tests: CommitPauseHooks freezes one
// transaction at an exact commit-protocol stage while other transactions run
// against it, making the ordering invariants deterministic instead of
// stress-dependent:
//   - a higher CID must wait for a lower CID's unregistered footprint;
//   - installed DV effects stay invisible until rows commit AND publish;
//   - a pending context behind a lower CID publishes nothing early.
class DependencyValidationCommitPauseTest : public BaseTest {
 protected:
  static constexpr auto kTableName = "dv_pause";

  void SetUp() override {
    BaseTest::SetUp();
    _column_definitions = TableColumnDefinitions{{"lhs", DataType::Int, false}, {"rhs", DataType::String, false}};
    _table = std::make_shared<Table>(_column_definitions, TableType::Data, std::optional<ChunkOffset>{ChunkOffset{100}},
                                     UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(kTableName, _table);
    _spec = DependencySpec{.kind = DependencyKind::FD,
                           .lhs_column_ids = {ColumnID{0}},
                           .rhs_column_ids = {ColumnID{1}},
                           .lhs_column_types = {DataType::Int},
                           .rhs_column_types = {DataType::String}};
    _build_cid = Hyrise::get().transaction_manager.last_commit_id();
    _table->build_and_attach_dependency_validator({ColumnID{0}}, {ColumnID{1}}, DependencyKind::FD, _build_cid);
  }

  void TearDown() override {
    CommitPauseHooks::clear();
    BaseTest::TearDown();
  }

  DependencyValidator validator() const {
    return _table->dependency_validation_api().front();
  }

  // Executes an Insert inside a fresh transaction but does NOT commit, so the
  // test decides on which thread and under which pause schedule the commit
  // runs.
  std::shared_ptr<TransactionContext> execute_insert(const std::vector<std::pair<int32_t, pmr_string>>& rows) {
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
    return context;
  }

  CommitID commit_insert(const std::vector<std::pair<int32_t, pmr_string>>& rows) {
    const auto context = execute_insert(rows);
    context->commit();
    return context->commit_id();
  }

  // Blocks the FIRST transaction that reaches `point`; later transactions (and
  // the same transaction at other points) pass through untouched. The blocked
  // thread holds no latches at any pause point, so freezing it is safe.
  class FirstArrivalPause {
   public:
    explicit FirstArrivalPause(const CommitPausePoint point) {
      CommitPauseHooks::set([this, point](const CommitPausePoint reached_point, const CommitID commit_id) {
        if (reached_point != point) {
          return;
        }
        auto lock = std::unique_lock<std::mutex>{_mutex};
        if (_reached) {
          return;  // Only the first arrival pauses.
        }
        _reached = true;
        _paused_commit_id = commit_id;
        _cv.notify_all();
        _cv.wait(lock, [this] {
          return _released;
        });
      });
    }

    // Returns the paused transaction's commit ID.
    CommitID wait_until_paused() {
      auto lock = std::unique_lock<std::mutex>{_mutex};
      _cv.wait(lock, [this] {
        return _reached;
      });
      return _paused_commit_id;
    }

    void release() {
      const auto lock = std::lock_guard<std::mutex>{_mutex};
      _released = true;
      _cv.notify_all();
    }

   private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _reached{false};
    bool _released{false};
    CommitID _paused_commit_id{UNSET_COMMIT_ID};
  };

  TableColumnDefinitions _column_definitions;
  std::shared_ptr<Table> _table;
  DependencySpec _spec;
  CommitID _build_cid{INITIAL_COMMIT_ID};
};

TEST_F(DependencyValidationCommitPauseTest, EveryPausePointFiresInProtocolOrder) {
  commit_insert({{1, "a"}});

  auto observed = std::vector<CommitPausePoint>{};
  auto observed_mutex = std::mutex{};
  CommitPauseHooks::set([&](const CommitPausePoint point, const CommitID /*commit_id*/) {
    const auto lock = std::lock_guard<std::mutex>{observed_mutex};
    observed.push_back(point);
  });

  commit_insert({{2, "b"}});
  CommitPauseHooks::clear();

  // A single unimpeded transaction traverses points 1-5 in protocol order;
  // the final pending-behind-lower-CID point requires contention and is
  // exercised below.
  ASSERT_EQ(observed.size(), 5u);
  EXPECT_EQ(observed[0], CommitPausePoint::CidAssignedFootprintNotRegistered);
  EXPECT_EQ(observed[1], CommitPausePoint::FootprintRegisteredPreparationNotStarted);
  EXPECT_EQ(observed[2], CommitPausePoint::EffectsInstalledRowsNotCommitted);
  EXPECT_EQ(observed[3], CommitPausePoint::RowsCommittedContextNotPending);
  EXPECT_EQ(observed[4], CommitPausePoint::DvVisibleWatermarkNotAdvanced);
}

TEST_F(DependencyValidationCommitPauseTest, HigherCidWaitsWhileLowerFootprintIsUnregistered) {
  commit_insert({{1, "a"}});

  // Freeze the first committer between CID assignment and footprint
  // registration -- the exact window the DV registration frontier exists for.
  auto pause = FirstArrivalPause{CommitPausePoint::CidAssignedFootprintNotRegistered};

  const auto lower = execute_insert({{1, "b"}});
  auto lower_thread = std::thread{[&] {
    lower->commit();
  }};
  const auto lower_cid = pause.wait_until_paused();

  auto higher_done = std::atomic<bool>{false};
  const auto higher = execute_insert({{2, "x"}});
  auto higher_thread = std::thread{[&] {
    higher->commit();
    higher_done.store(true);
  }};

  // The higher CID registers its footprint but must wait for the lower CID's
  // registration before its effects can install; nothing may become visible.
  std::this_thread::sleep_for(std::chrono::milliseconds{100});
  EXPECT_FALSE(higher_done.load());
  EXPECT_LT(validator().visible_commit_id(), lower_cid);

  pause.release();
  lower_thread.join();
  higher_thread.join();

  // Both commits applied in CID order; the verdict matches the oracle.
  const auto last = Hyrise::get().transaction_manager.last_commit_id();
  EXPECT_GE(last, lower_cid);
  EXPECT_EQ(validator().violation_count_at(last),
            batch_fd_violation_count(scan_visible_dependency_rows(*_table, _spec, last)));
  EXPECT_EQ(validator().violation_count_at(last), 1);
}

TEST_F(DependencyValidationCommitPauseTest, InstalledEffectsStayInvisibleUntilRowsCommitAndPublish) {
  const auto base_cid = commit_insert({{1, "a"}});

  // Freeze the committer AFTER its DV effects and history are installed in the
  // tree but BEFORE any row CID is written.
  auto pause = FirstArrivalPause{CommitPausePoint::EffectsInstalledRowsNotCommitted};

  const auto writer = execute_insert({{1, "b"}});
  auto writer_thread = std::thread{[&] {
    writer->commit();
  }};
  const auto writer_cid = pause.wait_until_paused();

  // Installed-but-unpublished metadata is invisible everywhere: the tree's
  // frontier still sits at the base snapshot, every answerable snapshot
  // reports the base verdict, and no row of the writer is visible.
  EXPECT_EQ(validator().visible_commit_id(), base_cid);
  EXPECT_TRUE(validator().holds_at(base_cid));
  EXPECT_TRUE(validator().holds_at(Hyrise::get().transaction_manager.last_commit_id()));
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, base_cid).size(), 1u);

  pause.release();
  writer_thread.join();

  // Row visibility and the DV verdict flip together at the writer's CID.
  EXPECT_EQ(scan_visible_dependency_rows(*_table, _spec, writer_cid).size(), 2u);
  EXPECT_EQ(validator().violation_count_at(writer_cid), 1);
  EXPECT_EQ(validator().violation_count_at(CommitID{writer_cid - 1}), 0);
}

TEST_F(DependencyValidationCommitPauseTest, DvVisibilityCompletesBeforeTheGlobalWatermarkAdvances) {
  const auto base_cid = commit_insert({{1, "a"}});
  auto pause = FirstArrivalPause{CommitPausePoint::DvVisibleWatermarkNotAdvanced};

  const auto writer = execute_insert({{1, "b"}});
  auto writer_thread = std::thread{[&] {
    writer->commit();
  }};
  const auto writer_cid = pause.wait_until_paused();

  // The tree's private visibility publication has completed, but Hyrise must
  // not hand out the writer CID until that publication is fully finished.
  EXPECT_EQ(Hyrise::get().transaction_manager.last_commit_id(), base_cid);
  EXPECT_EQ(validator().visible_commit_id(), base_cid);
  EXPECT_THROW(validator().violation_count_at(writer_cid), SnapshotNotVisible);

  const auto reader = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  EXPECT_EQ(reader->snapshot_commit_id(), base_cid);
  EXPECT_TRUE(validator().holds_at(reader->snapshot_commit_id()));
  reader->commit();

  pause.release();
  writer_thread.join();

  EXPECT_EQ(Hyrise::get().transaction_manager.last_commit_id(), writer_cid);
  EXPECT_EQ(validator().visible_commit_id(), writer_cid);
  EXPECT_EQ(validator().violation_count_at(writer_cid), 1);
}

TEST_F(DependencyValidationCommitPauseTest, PendingContextBehindLowerCidPublishesNothingEarly) {
  const auto base_cid = commit_insert({{1, "a"}});

  // Freeze the lower committer pre-row-commit, then let a higher committer run
  // all the way to pending. The higher context must reach
  // ContextPendingBehindLowerCid and stay unpublished.
  auto lower_pause = FirstArrivalPause{CommitPausePoint::EffectsInstalledRowsNotCommitted};
  auto pending_reached = std::atomic<bool>{false};

  const auto lower = execute_insert({{1, "b"}});
  auto lower_thread = std::thread{[&] {
    lower->commit();
  }};
  const auto lower_cid = lower_pause.wait_until_paused();

  CommitPauseHooks::set([&](const CommitPausePoint point, const CommitID /*commit_id*/) {
    if (point == CommitPausePoint::ContextPendingBehindLowerCid) {
      pending_reached.store(true);
    }
  });
  // NOTE: replacing the observer would disable the lower transaction's pause
  // predicate, but that transaction is already blocked inside the previous
  // observer's wait and only needs its release() below.

  const auto higher = execute_insert({{5, "z"}});
  auto higher_done = std::atomic<bool>{false};
  auto higher_thread = std::thread{[&] {
    higher->commit();
    higher_done.store(true);
  }};

  // The higher CID becomes pending but cannot publish behind the frozen lower
  // CID: the watermark, the tree frontier, and every verdict stay at base.
  while (!pending_reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  EXPECT_FALSE(higher_done.load());
  EXPECT_LT(Hyrise::get().transaction_manager.last_commit_id(), lower_cid);
  EXPECT_EQ(validator().visible_commit_id(), base_cid);
  EXPECT_TRUE(validator().holds_at(Hyrise::get().transaction_manager.last_commit_id()));

  lower_pause.release();
  lower_thread.join();
  higher_thread.join();

  // Publication happened in CID order and the final verdict matches the
  // oracle: (1,"a"), (1,"b"), (5,"z") -> exactly one FD violation.
  const auto last = Hyrise::get().transaction_manager.last_commit_id();
  EXPECT_EQ(validator().violation_count_at(last),
            batch_fd_violation_count(scan_visible_dependency_rows(*_table, _spec, last)));
  EXPECT_EQ(validator().violation_count_at(last), 1);
}

}  // namespace hyrise::dv_tree
