#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

namespace hyrise::dv_tree {

// The core keeps commit IDs in a plain integral representation. Hyrise's
// strong CommitID is used at every public boundary and converted only through
// the checked helpers below.
using InternalCommitID = uint64_t;
inline constexpr InternalCommitID INVALID_INTERNAL_COMMIT_ID = 0;
inline constexpr InternalCommitID MAX_VALID_INTERNAL_COMMIT_ID =
    static_cast<InternalCommitID>(static_cast<CommitID::base_type>(MAX_COMMIT_ID)) - 1;

struct InvalidDVCommitID : std::logic_error {
  using std::logic_error::logic_error;
};

// Real transaction CIDs exclude UNSET_COMMIT_ID and Hyrise's reserved
// MAX_COMMIT_ID. Snapshot/baseline conversion additionally permits UNSET (0).
InternalCommitID to_internal_commit_id(CommitID cid);
InternalCommitID to_internal_snapshot_id(CommitID cid);
CommitID to_hyrise_commit_id(InternalCommitID cid);

enum class DependencyKind { FD, OD };

struct MissingDependencyRow : std::logic_error {
  using std::logic_error::logic_error;
};

struct CommitOrderViolation : std::logic_error {
  using std::logic_error::logic_error;
};

struct SnapshotNotVisible : std::logic_error {
  using std::logic_error::logic_error;
};

// Thrown by public snapshot queries (e.g. DVTree::violation_count_at) when the
// requested snapshot has already scrolled out below the retained history
// horizon. Public because callers of those public methods must be able to
// catch it; kept here rather than in the internal versioned history header.
struct SnapshotHorizonViolation : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct Transaction {
  struct Op {
    bool is_delete;
    std::string lhs_norm;
    std::string rhs_norm;
  };

  std::vector<Op> ops;

  void insert(std::string lhs_norm, std::string rhs_norm) {
    ops.push_back({false, std::move(lhs_norm), std::move(rhs_norm)});
  }

  void remove(std::string lhs_norm, std::string rhs_norm) {
    ops.push_back({true, std::move(lhs_norm), std::move(rhs_norm)});
  }

  void update(std::string lhs_norm, std::string old_rhs_norm, std::string new_rhs_norm) {
    remove(lhs_norm, std::move(old_rhs_norm));
    insert(std::move(lhs_norm), std::move(new_rhs_norm));
  }
};

}  // namespace hyrise::dv_tree
