#pragma once

#include <atomic>
#include <functional>
#include <mutex>

#include "types.hpp"

namespace hyrise::dv_tree {

// Deterministic pause points along the transaction commit path. This is the
// Phase 8/14 adversarial scheduling instrumentation: tests install an observer
// and block inside it to freeze one transaction at an exact protocol stage
// while other transactions run against it. Production never installs an
// observer, so each point costs a single relaxed atomic load.
enum class CommitPausePoint {
  // A Hyrise CID exists, but the DV footprint is not registered yet: the
  // registration frontier must make any higher CID wait for this commit.
  CidAssignedFootprintNotRegistered,
  // The footprint is registered; effect preparation/installation has not
  // started.
  FootprintRegisteredPreparationNotStarted,
  // DV effects and history are installed in the trees, but row MVCC CIDs are
  // not written: the installed metadata must not be observable anywhere.
  EffectsInstalledRowsNotCommitted,
  // Row CIDs are written, but the commit context is not pending yet.
  RowsCommittedContextNotPending,
  // DV visibility is published, but the global Hyrise watermark has not yet
  // advanced. New snapshots must still observe the predecessor CID.
  DvVisibleWatermarkNotAdvanced,
  // The context is pending but unpublished because a lower CID has not
  // published: neither rows nor DV metadata may become visible.
  ContextPendingBehindLowerCid,
};

// Global observer registry. Tests install one observer, drive their
// transactions on separate threads, and block inside the observer callback to
// realize a schedule. The observer runs on the committing thread at a point
// where it holds no latches, so blocking there is safe.
class CommitPauseHooks {
 public:
  using Observer = std::function<void(CommitPausePoint, CommitID)>;

  static void set(Observer observer);
  static void clear();

  // Called from the commit path; no-op unless an observer is installed.
  static void notify(CommitPausePoint point, CommitID commit_id);

 private:
  static std::atomic<bool> _armed;
  static std::mutex _mutex;
  static Observer _observer;
};

}  // namespace hyrise::dv_tree
