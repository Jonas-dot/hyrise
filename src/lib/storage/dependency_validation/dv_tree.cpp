#include "storage/dependency_validation/dv_tree.hpp"

#include <stdexcept>

#include "storage/dependency_validation/dv_tree_impl.hpp"

namespace hyrise::dv_tree {

class DVTree::Impl final : public DVTreeImpl {
 public:
  using DVTreeImpl::DVTreeImpl;
};

struct DVTree::CommitTicket::Impl {
  explicit Impl(DVTreeImpl::CommitTicket internal_ticket) : ticket(std::move(internal_ticket)) {}

  DVTreeImpl::CommitTicket ticket;
};

DVTree::CommitTicket::CommitTicket() = default;
DVTree::CommitTicket::~CommitTicket() = default;
DVTree::CommitTicket::CommitTicket(CommitTicket&&) noexcept = default;
DVTree::CommitTicket& DVTree::CommitTicket::operator=(CommitTicket&&) noexcept = default;

DVTree::CommitTicket::CommitTicket(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

CommitID DVTree::CommitTicket::commit_id() const {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  return to_hyrise_commit_id(impl_->ticket.commit_id());
}

void DVTree::CommitTicket::stage(Transaction fragment) {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.stage(std::move(fragment));
}

void DVTree::CommitTicket::insert(std::string lhs_norm, std::string rhs_norm) {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.insert(std::move(lhs_norm), std::move(rhs_norm));
}

void DVTree::CommitTicket::remove(std::string lhs_norm, std::string rhs_norm) {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.remove(std::move(lhs_norm), std::move(rhs_norm));
}

void DVTree::CommitTicket::update(std::string lhs_norm, std::string old_rhs_norm, std::string new_rhs_norm) {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.update(std::move(lhs_norm), std::move(old_rhs_norm), std::move(new_rhs_norm));
}

void DVTree::CommitTicket::seal() {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.seal();
}

void DVTree::CommitTicket::abort() {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.abort();
}

void DVTree::CommitTicket::wait_until_applied() const {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.wait_until_applied();
}

void DVTree::CommitTicket::wait_until_visible() const {
  if (!impl_)
    throw CommitOrderViolation("commit ticket has no implementation");
  impl_->ticket.wait_until_visible();
}

DVTree::DVTree(DependencyKind kind, std::size_t history_soft_capacity, const CommitID initial_visible_cid)
    : impl_(std::make_unique<Impl>(kind, history_soft_capacity, to_internal_snapshot_id(initial_visible_cid))) {}

DVTree::~DVTree() = default;

DVTree::CommitTicket DVTree::begin_commit() {
  return CommitTicket(std::make_unique<CommitTicket::Impl>(impl_->begin_commit()));
}

DVTree::CommitTicket DVTree::begin_commit(const CommitID commit_id) {
  return CommitTicket(std::make_unique<CommitTicket::Impl>(impl_->begin_commit(to_internal_commit_id(commit_id))));
}

void DVTree::advance_registration_frontier(const CommitID commit_id) {
  impl_->advance_registration_frontier(to_internal_commit_id(commit_id));
}

void DVTree::advance_visibility_frontier(const CommitID commit_id) {
  impl_->advance_visibility_frontier(to_internal_commit_id(commit_id));
}

CommitID DVTree::apply_commit(const Transaction& transaction) {
  return to_hyrise_commit_id(impl_->apply_commit(transaction));
}

bool DVTree::holds() const {
  return impl_->holds();
}

int64_t DVTree::violations() const {
  return impl_->violations();
}

bool DVTree::holds_at(const CommitID snapshot) const {
  return impl_->holds_exact(to_internal_snapshot_id(snapshot));
}

int64_t DVTree::violation_count_at(const CommitID snapshot) const {
  return impl_->violations_exact(to_internal_snapshot_id(snapshot));
}

bool DVTree::holds_exact(const CommitID snapshot) const {
  return holds_at(snapshot);
}

int64_t DVTree::violations_exact(const CommitID snapshot) const {
  return violation_count_at(snapshot);
}

CommitID DVTree::visible_commit_id() const {
  return to_hyrise_commit_id(impl_->visible_commit_id());
}

std::optional<DVTree::EntrySnapshot> DVTree::snapshot_entry(std::string_view lhs_norm) const {
  const auto snapshot = impl_->snapshot_entry(lhs_norm);
  if (!snapshot)
    return std::nullopt;
  EntrySnapshot result;
  result.lhs = snapshot->lhs;
  result.rhs_counts = snapshot->rhs_counts;
  result.local_violations = snapshot->local_violations;
  result.neighbor_violation = snapshot->neighbor_violation;
  result.version = to_hyrise_commit_id(snapshot->version);
  return result;
}

DVTree::MemoryStatistics DVTree::memory_statistics() const {
  return impl_->memory_statistics();
}

#ifdef DV_TESTING
uint64_t DVTree::max_concurrent_od_transactions_for_test() const {
  return impl_->max_concurrent_od_transactions_for_test();
}

uint64_t DVTree::optimistic_reprepare_count_for_test() const {
  return impl_->optimistic_reprepare_count_for_test();
}

uint64_t DVTree::reservation_wait_count_for_test() const {
  return impl_->reservation_wait_count_for_test();
}

std::size_t DVTree::prepared_key_count_for_test(const CommitID cid) const {
  return impl_->prepared_key_count_for_test(to_internal_commit_id(cid));
}

bool DVTree::effects_installed_for_test(const CommitID cid) const {
  return impl_->effects_installed_for_test(to_internal_commit_id(cid));
}

const VersionedViolationHistory& DVTree::history() const {
  return impl_->history();
}

int64_t DVTree::violations_live_uncommitted() const {
  return impl_->violations_live_uncommitted();
}

DependencyEntry* DVTree::find(std::string_view lhs_norm) const {
  return impl_->find(lhs_norm);
}

DependencyEntry* DVTree::first_entry() const {
  return impl_->first_entry();
}

std::string_view DVTree::rhs_bytes(RhsRef ref) {
  return DVTreeImpl::rhs_bytes(ref);
}

void DVTree::compute_verdict() {
  impl_->compute_verdict();
}
#endif

void DVTree::set_lowest_active_snapshot(const CommitID cid) {
  impl_->set_lowest_active_snapshot(to_internal_snapshot_id(cid));
}

void DVTree::clear_lowest_active_snapshot() {
  impl_->clear_lowest_active_snapshot();
}

}  // namespace hyrise::dv_tree
