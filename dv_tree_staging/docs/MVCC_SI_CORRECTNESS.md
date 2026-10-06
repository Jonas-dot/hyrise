# MVCC and snapshot-isolation correctness contract

## Scope

DV-Tree (`DVTree`) is a versioned dependency-verdict index, not a second row
store. Mutable RHS multisets are always-latest metadata protected by short entry
latches and CID-aware logical footprint reservations. MVCC history stores
cumulative scalar dependency-violation change points. For any visible snapshot
`s`:

```text
violations_exact(s) = total_after[max retained change-point CID <= s]
holds_exact(s)       = violations_exact(s) == 0
```

The host database remains responsible for row visibility, write-conflict
detection, transaction rollback before commit, and assigning snapshot/commit
IDs. The index answers whether the committed row prefix visible at a CID obeys
the configured FD or OD.

## Normalized-key semantics

Every index has one immutable ascending ordering policy, independent of DBMS
session configuration:

```text
-Infinity < finite negatives < canonical zero < finite positives < +Infinity < NaN < NULL
```

The displayed chain is per floating/nullable column; ordinary values retain
their type-specific order. The exact rules are:

- DuckDB radix encoding is used for floats and doubles. All NaN payloads are
  equal and occupy the maximum non-NULL key; both signed zeros are equal.
- Every column has a validity marker. Valid values use `0x01`; NULL uses the
  larger, single-byte `0x02`, implementing `ASC NULLS LAST`.
- NULL participates with literal/`IS NOT DISTINCT FROM` semantics. Two NULLs
  are equal, NULL differs from every non-NULL value, and a composite containing
  NULL is not discarded.
- Variable strings are self-delimiting and binary-safe, including embedded zero
  and `0xFF` bytes.

Consequently FD distinct-RHS counting treats all NaNs as one value and all NULLs
as one value. OD uses the same total byte order for predecessor/successor terms.
An adapter that defines dependencies by excluding NULL-containing rows must do
so explicitly before staging index operations; it must never switch the byte
encoding of a live index.

## Linearization and locking

The standalone concurrent path starts with `begin_commit()`, which reserves a
consecutive CID and returns a move-only `CommitTicket`. At `seal()`, the ticket
constructs its complete normalized footprint: sorted touched LHS keys for FD,
or a conservative predecessor–successor interval for OD. Footprints become
registered in consecutive CID order, closing the ambiguous-pending-map case:
CID 11 cannot treat a not-yet-sealed CID 10 as conflict-free merely because its
footprint has not arrived. Dropping an unsealed ticket aborts it; an explicit
empty seal or abort retires it as a zero-delta CID.

For every resource, a transaction checks registered, incomplete lower-CID
footprints. It parks on `registered_cv_` when they intersect and holds no page,
topology, or entry latch while waiting.

A registered OD interval describes the neighbour structure as it was at seal
time, so a lower CID that tombstones the groups in between can make previously
separated groups neighbours and widen the window a transaction actually
prepares. The registered interval is therefore only an early filter. The
authoritative OD check runs after preparation, against the prepared window's key
range, and repeats after every reprepare. It is sound because every way a lower
CID can disturb a term inside that window -- changing a compared group's
maximum, changing the successor group's minimum, or tombstoning/resurrecting a
group between them -- runs through one of that CID's own touched keys, and a
footprint's key list is a pure function of its transaction and never goes stale.
Waits still only ever point from higher to lower CIDs, so the wait graph follows
CID order and cannot cycle. An FD transaction with several keys may
privately prepare currently free keys while another key waits. OD treats its
conservative interval as one scheduling resource. Disjoint higher CIDs may
prepare and physically install before a lower CID completes.

Preparation locks `DependencyEntry` objects only long enough to copy their maps,
counters, and versions. Map updates and delta calculation live in private
`CommitEnvelope` state. Installation reacquires the complete entry set in key
order, validates every version, and for OD validates the topology epoch while
the topology latch is shared. It then applies one complete, preallocated,
non-failing metadata batch and releases physical locks. Versions remain a safety
net for topology/structural races; ordinary overlap is prevented before stale
preparation by the footprint reservation.

The metadata-effect linearization point is that complete batch installation.
The transaction then marks its envelope complete and releases its logical
reservations. A separate short `advance_completed_prefix()` drain consumes only
consecutive completed/aborted CIDs, appends cumulative history change points,
and release-stores the visible CID. Thus an entry at disjoint physical version
11 may exist while exact snapshot visibility remains at CID 9; snapshot verdict
APIs do not expose CID 11 until CID 10 and CID 11 form a completed prefix.

This closes M3 for arbitrary FD and OD ticket concurrency: same-entry and
overlapping-interval effects follow CID order, disjoint work can install
concurrently, and history remains a consecutive CID prefix without suffix
repair. `apply_commit(transaction)` uses this same ticket/reservation path; no
second mutation or externally driven publication path exists.

## Future Hyrise integration

The chosen integration does not add a DV-specific publisher to Hyrise. Hyrise
continues to own row visibility, CID allocation, and its existing consecutive
`_last_commit_id` advancement. DV work must finish before the transaction marks
its existing `CommitContext` pending.

The adapter must collect every row/operator fragment into one transaction-owned
batch, build complete footprints for every affected dependency tree, and make
CID assignment plus footprint registration unambiguous. It then runs the same
reservation/preparation/version-validation protocol and installs the complete
DV batch before the existing pending transition. Transactions without DV work
follow the ordinary Hyrise path.

The standalone proves atomic installation within one DV-Tree. Hyrise must add a
transaction-wide multi-tree boundary: preallocate and validate every affected
tree before any installation, followed by a non-failing install phase, or use a
coordinated marker/rollback protocol. One dependency tree cannot by itself make
several tree updates atomic or prevent the DBMS from exposing `last_commit_id`.

Construct an empty index with `initial_visible_cid` equal to the host's current
visible CID. Loading an existing table is deliberately not a second public
mutation path in this standalone: the future Hyrise adapter must define a
quiescent bootstrap/rebuild operation and its creation-CID contract. Snapshots
older than index creation are outside that contract. Feed
`TransactionManager::get_lowest_active_snapshot_commit_id()` into
`set_lowest_active_snapshot()`; clear it when no active snapshot exists.

The history constructor's capacity is a soft retained-size target, not a limit
on the lifetime of an active snapshot. Cumulative totals older than the
lowest-active CID are folded into a baseline. If the protected interval contains
more non-zero change points than the target, the deque grows dynamically;
moving or clearing the horizon folds the newly obsolete prefix and shrinks the
deque toward the target. Readers share a read lock, while horizon changes and
the short completion drain take the exclusive side.

Public no-argument verdict reads query history only through the visible
`visible_commit_id()` watermark. A
prepared or disjoint already-installed future CID is therefore invisible to
snapshot verdicts until the completion-prefix store.

## Error and lifetime guarantees

The production transaction APIs validate deletes and allocate/copy their final
maps before batch installation. A transient version/topology mismatch is
reprepared. A definitive error is reported by `wait_until_visible()` and leaves
that ticket's CID incomplete, so the visible prefix stops at the preceding CID.
Explicit abort converts the reservation to a no-op, releases waiters, and
resumes the completion drain. `apply_commit()` automatically performs this
abort before rethrowing its error. Protected history never rejects a commit
merely because the soft target was exceeded. An unused empty structural entry or unreferenced
append-only RHS allocation may remain after a failed preparation; neither
contributes to logical index state.

`DependencyEntry` objects and RHS byte allocations never move or disappear
while the index lives. B-tree nodes are never merged or reclaimed. OLC readers
therefore validate stable pointers without requiring epochs.

This is the intentional ownership boundary, not deferred core-index work.
Online merging, reclamation, and vacuuming are out of scope. The host DBMS owns
vacuum/rebuild policy and can recover retained tombstones, RHS storage, and split
pages by replacing the complete index at a quiescent boundary.

`find()`, `first_entry()`, raw `DependencyEntry` fields, history inspection, and
batch recomputation exist only in `DV_TESTING` builds. Concurrent diagnostic
metadata inspection uses `snapshot_entry()`. Snapshot-isolation callers use
`holds_exact()`/`violations_exact()`; clamping historical overloads are not part
of the public DV-Tree API.

## Proven invariants and regression gates

- FD: `local(k) = max(0, distinct_rhs(k) - 1)`.
- OD: `neighbor(k)` compares `rhs_max(k)` with the minimum RHS of the next
  non-empty key, skipping any number of tombstones.
- Global: committed violations equal the sum of every local and neighbour term.
- Normalization: NaN and signed-zero canonicalization, `ASC NULLS LAST`, and
  literal nullable FD/OD behavior match the fixed key contract above.
- History: each transaction's exact global delta produces its CID's cumulative
  `total_after`; zero-delta CIDs still advance visibility without consuming a
  retained entry, and a protected active window grows rather than losing data.
- Coordinator: complete footprints are registered before execution; overlapping
  FD keys/OD intervals wait for lower CIDs without physical latches; disjoint
  work may install out of order; the completion/history prefix cannot skip a
  CID; abort/no-op and definitive-error recovery close gaps explicitly.
- Per-CID attribution under OD concurrency: `violations_exact(cid)` equals a
  sequential replay of the captured CID order for *every* committed prefix, not
  only the final state. Gated by
  `test_od_stale_disjoint_intervals_deterministic_prefix` (deterministic
  stale-interval reproducer), its 300x racing variant, and
  `test_concurrent_od_every_prefix_matches_captured_replay` (randomized
  per-prefix differential). FD is structurally exempt: FD footprints are exact
  key sets that cannot go stale and FD terms are per-entry local, so FD deltas
  always commute.
- C5: A1 through A6 each have a named regression, including forced restart paths.
- Differential gates compare incremental FD/OD state with batch recomputation
  and compare concurrent OD execution with captured-CID sequential replay.
- Debug, release, ASan/UBSan, repeated threaded runs, and TSan are required before
  integration changes are accepted.

## Standalone audit conclusion

Within the stated aggregate-verdict scope, the standalone index now has a
defined metadata linearization point, CID-aware complete-footprint reservation,
private preparation with installation-time version validation, atomic per-tree
FD/OD batches, exact cumulative committed-prefix history, exact M3 attribution,
strong logical exception safety, stable optimistic-reader lifetimes, and named
C5 regressions. No known standalone correctness item from Phases A through C5
remains open.

This does not mean the Hyrise integration is already complete. The adapter must
still implement transaction-wide fragment collection, atomic CID/footprint
registration, a non-failing or recoverable multi-tree batch, lowest-active-
snapshot propagation, quiescent initial construction, conversion between
Hyrise's strong CID type and this standalone type, and lifetime ownership inside
`Table`. Hyrise's existing visible-CID publisher need not be changed. These are
integration obligations rather than missing per-tree index algorithms.
