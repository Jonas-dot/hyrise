#include "storage/dependency_validation/dv_types.hpp"

namespace hyrise::dv_tree {

InternalCommitID to_internal_commit_id(const CommitID cid) {
  const auto raw_cid = static_cast<CommitID::base_type>(cid);
  const auto reserved_cid = static_cast<CommitID::base_type>(MAX_COMMIT_ID);
  if (raw_cid == static_cast<CommitID::base_type>(UNSET_COMMIT_ID) || raw_cid >= reserved_cid) {
    throw InvalidDVCommitID{"DV-Tree requires a real Hyrise commit ID below MAX_COMMIT_ID"};
  }
  return static_cast<InternalCommitID>(raw_cid);
}

InternalCommitID to_internal_snapshot_id(const CommitID cid) {
  const auto raw_cid = static_cast<CommitID::base_type>(cid);
  const auto reserved_cid = static_cast<CommitID::base_type>(MAX_COMMIT_ID);
  if (raw_cid >= reserved_cid) {
    throw InvalidDVCommitID{"DV-Tree snapshot ID must be below MAX_COMMIT_ID"};
  }
  return static_cast<InternalCommitID>(raw_cid);
}

CommitID to_hyrise_commit_id(const InternalCommitID cid) {
  if (cid > MAX_VALID_INTERNAL_COMMIT_ID) {
    throw InvalidDVCommitID{"internal DV-Tree commit ID is not representable as a real Hyrise commit ID"};
  }
  return CommitID{static_cast<CommitID::base_type>(cid)};
}

}  // namespace hyrise::dv_tree
