#include "storage/dependency_validation/dependency_validation_test_hooks.hpp"

#include <mutex>
#include <utility>

namespace hyrise::dv_tree {

std::atomic<bool> CommitPauseHooks::_armed{false};
std::mutex CommitPauseHooks::_mutex;
CommitPauseHooks::Observer CommitPauseHooks::_observer;

void CommitPauseHooks::set(Observer observer) {
  const auto guard = std::lock_guard<std::mutex>{_mutex};
  _observer = std::move(observer);
  _armed.store(static_cast<bool>(_observer), std::memory_order_release);
}

void CommitPauseHooks::clear() {
  set({});
}

void CommitPauseHooks::notify(const CommitPausePoint point, const CommitID commit_id) {
  if (!_armed.load(std::memory_order_relaxed)) {
    return;
  }
  // Copy the observer so it is invoked WITHOUT holding the registry mutex --
  // observers block to realize test schedules.
  auto observer = Observer{};
  {
    const auto guard = std::lock_guard<std::mutex>{_mutex};
    observer = _observer;
  }
  if (observer) {
    observer(point, commit_id);
  }
}

}  // namespace hyrise::dv_tree
