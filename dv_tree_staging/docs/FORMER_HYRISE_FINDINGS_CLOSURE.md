# Former Hyrise finding closure matrix

This matrix maps every finding in
the former Hyrise OLC B-tree review to the
standalone `playground_btree_cpp` design. "Closed" means the defect is absent
from, or has a tested correction in, this standalone implementation. It does
not mean that the old Hyrise implementation has already been replaced.

The review mixed three kinds of concerns. Core index correctness must be closed
here. Hyrise lifecycle/API concerns become adapter gates. Reclamation and merge
policy are explicitly owned by the DBMS and are not standalone acceptance
criteria, provided no object is freed while OLC readers can observe it.

## Critical and high findings

| Finding | Standalone disposition | Evidence and integration consequence |
| --- | --- | --- |
| E1: unbounded determinant payload | **Closed** | A leaf stores one fixed-size `DependencyEntry*`; the unbounded RHS multiset is out of line. `test_fd_metadata_is_out_of_line` exercises thousands of RHS values for one LHS. |
| E2: fixed TLS-buffer overflow | **Closed** | Metadata updates use owned maps and `StableRhsStorage`; there is no serialized variable-length payload or 4 KiB metadata scratch buffer. |
| A1: optimistic parse before validation | **Closed** | Readers copy an immutable published 4 KiB `PageBody`, validate before dereferencing a child, and return a stable payload pointer only after the leaf validates. A stale child whose post-split fence prefix no longer contains the old-parent search key throws `RestartOperation` rather than asserting before validation. `test_a1_torn_leaf_snapshot_forces_lookup_restart` and `test_stale_split_prefix_forces_restart` cover both edges. |
| A2: sibling pointer used after validation | **Closed** | Neighbor lookup snapshots sibling versions and links, validates them before returning stable entry pointers, and restarts on change. `test_a2_stale_sibling_snapshot_forces_neighbor_restart` is the forced regression. |
| B1: history corrupt after 512 commits | **Closed** | `VersionedViolationHistory` stores cumulative `{CID,total_after}` change points and folds only history older than the lowest-active snapshot. Its capacity is soft: a protected window grows dynamically instead of rejecting or overwriting a commit, then compacts after the horizon advances. Sequential and concurrent growth/folding regressions cover this. |
| B3: inline OD count/flag drift | **Closed** | There is one OD delta path. It recomputes affected local and neighbor terms from staged final maps while the complete interval is locked; the independent batch oracle checks the ledger. |
| E3: OD middle insert double count | **Closed** | Interval recomputation removes the old boundary and installs both new boundaries as one transaction. Covered by OD composite, tombstone, and randomized differential tests. |
| E4: incomplete single-column OD remove | **Closed** | Scalar and composite keys enter the same normalized-key OD implementation and the same remove algorithm. |
| A3: missed cross-leaf OD boundary | **Closed** | Dependency adjacency is maintained in a stable metadata chain and locked by affected intervals, independent of page boundaries. `test_a3_cross_leaf_od_boundary_matches_batch_recompute` forces the former case. |
| C1: composite range bounds use column 0 | **Not applicable to the validator shell** | This project has no `AbstractChunkIndex::_lower_bound/_upper_bound` sorted-array API. Complete composite normalized bytes are compared by the B-tree. A future Hyrise adapter must not retain the old general-index range implementation as part of validation. |
| C3: `0xFF` corruption and length hazards | **Closed with an explicit LHS cap** | String framing is binary-safe for zero and `0xFF`; RHS bytes are out of line. The TUM page formula defines an always-active maximum LHS key length and `test_oversized_key_fails_before_upstream_assert` verifies loud failure in debug and release. |
| B4: correct transaction path dead in Hyrise | **Closed standalone; Hyrise switch pending** | Move-only `CommitTicket`s register complete FD-key/OD-interval footprints, wait only on lower-CID intersections, validate versions, and install complete batches. The Hyrise adapter must collect transaction-wide fragments, use this publisher-free boundary, and remove the old per-row mutation route. |

## Medium findings

| Finding | Standalone disposition | Evidence and integration consequence |
| --- | --- | --- |
| B2: global history lock and O(n) work per row | **Resolved for the intended commit path** | Row fragments are batched per transaction and history is updated once by the short consecutive completion drain. Readers take shared locks and binary-search cumulative totals; completion/folding takes one short exclusive lock. Query cost is logarithmic in retained change points. |
| A4: size double-count on restart | **Closed** | Entry accounting happens only after a successful insertion. `test_a4_insert_restart_counts_entry_once` forces an insert restart and checks the count. |
| A5: remove/reinsert visibility window | **Closed** | Updates are staged as final per-entry maps and swapped while all affected entry locks remain held. `test_a5_remove_reinsert_is_commit_atomic` checks committed readers. |
| A6: stale `prev_leaf` during split | **Closed** | Split relinking locks the parent and target, then validated-try-locks the affected previous leaf; contention restarts and releases the held guards rather than blocking the latch chain. `test_a6_three_leaf_relink_keeps_exact_neighbors` and the split stress test verify the chain. |
| C4: signed-zero divergence | **Closed** | Float and double normalization canonicalizes both zero signs and all NaNs using the fixed DuckDB-style radix policy. |
| C6: lazy validation-pointer race | **Closed by construction; old Hyrise members must go** | A `DVTree` owns one eagerly constructed tree and metadata state. The adapter must replace, rather than lazily initialize alongside, `_val_tree` and `_multi_val_state`. |
| B5: repeated full scans/traversals | **Closed for metadata** | Each RHS group is an ordered map, giving direct min/max, and one prepared transaction computes the complete affected set. OD interval tests demonstrate concurrency for disjoint ranges. Benchmarking may still motivate data-structure tuning. |
| D1: diverged copied implementations | **Closed inside the new project; deployment pending** | FD/OD, scalar/composite, and concurrent/standalone behavior share one implementation. Hyrise must compile or wrap this code and delete the former duplicate validator, not copy fixes into both. |
| D2: no merge/reclamation | **Accepted out of scope** | Nodes, entries, tombstones, and RHS allocations remain stable until index destruction/rebuild, which makes current OLC pointer use safe. The host DBMS owns quiescent vacuum/rebuild; online merge and reclamation are not claimed. |
| C5: two validator trees per index | **Closed standalone; old Hyrise members must go** | One `DVTree`, configured once as FD or OD, handles every arity through normalized composite keys. The adapter must not preserve the two old tree members as fallbacks. |
| C7: silent NULL shortcut | **Closed** | Every column has a validity marker and uses documented literal/`IS NOT DISTINCT FROM`, `ASC NULLS LAST` semantics. An SQL policy that excludes NULL rows must be an explicit adapter prefilter. |
| E5: production wiring | **Hyrise integration gate** | The standalone ticket coordinator proves complete-footprint registration, per-key/interval waiting, disjoint early installation, version/epoch revalidation, consecutive cumulative-history completion, and abort/no-op retirement. Hyrise still needs transaction-wide fragment ownership, atomic CID/footprint registration, multi-tree batch atomicity, initial CID, and lowest-active propagation; end-to-end Hyrise MVCC/SI compliance is not claimed yet. |

## Low and completeness findings

| Finding | Standalone disposition | Evidence and integration consequence |
| --- | --- | --- |
| B6: CID zero overloaded | **Closed** | `INVALID_COMMIT_ID` is reserved and production commit APIs reject CID zero; actual commits begin above it. |
| C2: B-tree shell does not index | **Avoided** | This is deliberately a dependency-verdict component and its normalized LHS keys really are stored in the B-tree. It does not pretend to implement the unrelated `AbstractChunkIndex` range API. |
| C8: understated memory accounting | **Closed standalone; Hyrise allocator integration pending** | `DVTree::memory_statistics()` concurrently accounts live inner/leaf pages and published images, stable entries/LHS bytes, RHS map and append-only storage, retained history, active envelopes/footprints, and current/peak private preparations. A Hyrise adapter must still translate these portable logical values into allocator-exact charged bytes where required. |
| D3: vestigial successor callback | **Closed** | The adapter exposes only `neighbors(key)` and no unused key callback. |
| D4: 500 us retry sleep | **Closed** | OLC retry backoff yields first and caps randomized sleep at 50 us. |
| D5: missing failure-mode tests | **Closed standalone** | Named tests cover oversized data, more-than-capacity history, protected growth, forced A1/A2/A4 restarts, A3/A6 page boundaries, A5 atomicity, recursive inner-node splits with concurrent readers/writers, page-image allocation failure, reverse-order many-thread FD/OD completion, overlapping reservation waits, mixed-footprint preparation, disjoint early installation, abort/no-op/error recovery, tombstones, snapshots, and differential recomputation under sanitizer targets. |
| A7: VLA | **Closed** | Separator and restored-key buffers use compile-time `std::array` bounds. |
| E6: dead neighbor/transaction code | **Closed standalone; cleanup required in Hyrise** | The obsolete standalone seqlock and overlapping neighbor APIs are absent. Hyrise integration must remove its unused `DVTransactionContext`/neighbor routes when replacing the validator. |
| E7: refcount-only OD remove work | **Closed** | One remove path stages the final exact RHS refcount map; neighbor terms change only when distinctness/min/max changes. |
| E8: 56-bit version overflow | **Closed** | `Node::write_unlock()` masks the increment with `VERSION_MASK`, so wrap cannot enter the lock-state byte; `test_olc_version_wrap_preserves_lock_state` forces the boundary and verifies the next readable version is zero. |

## Acceptance boundary

There is no remaining known standalone verdict-correctness finding from the
former review. The remaining work before making the same statement about a
Hyrise build is integration work:

1. replace the old `_val_tree`/`_multi_val_state` mutation routes with one owned
   `DVTree`;
2. collect all row/operator fragments per transaction, build complete
   footprints, and atomically associate their registration with the assigned CID;
3. execute publisher-free conflict reservation and one non-failing/recoverable
   batch across every affected dependency tree before the existing
   `CommitContext` becomes pending; Hyrise's visibility publisher stays unchanged;
4. propagate the lowest active snapshot and construct/rebuild at a quiescent
   boundary with the correct initial CID;
5. add Hyrise-native lifecycle, end-to-end MVCC/SI, and memory-accounting tests.

Online merging/reclamation remains explicitly outside this boundary. A future
implementation may add it only together with a safe epoch/hazard scheme; it is
not required for logical correctness while objects are retained for the full
index lifetime.

## What "old Hyrise validation paths" means

This phrase refers specifically to the dependency-validation mutation routes in
the current Hyrise branch, not to every Hyrise index lookup:

- `Insert::_on_commit_records` and `Delete::_on_commit_records` call
  `insert_entry_for_validation`/`delete_entry_for_validation` once per row.
- `BTreeOLCIndex` then branches by arity. One-LHS/one-RHS input mutates
  `olc_detail::DependencyValidatingBTree`; all other arities mutate the separate
  `MultiValidationState`.
- `global_violation_count()` sums both optional states. Thus the two independent
  implementations and their histories remain live production possibilities.
- `olc_detail::DVTransactionContext` contains a batching/deferred algorithm, but
  the operator callbacks above do not use it.

Replacing those paths does not require deleting Hyrise's ordinary
`AbstractChunkIndex` lookup APIs. It means that dependency registration owns one
new `DVTree`; row callbacks only append normalized operations to that
transaction's fragment collector; transaction completion seals the collector;
CID assignment registers the complete footprints; and the transaction finishes
its DV reservations, validation, and complete multi-tree batch before marking
its existing `CommitContext` pending.

The current `TransactionManager::_try_increment_last_commit_id()` can remain
unchanged. Its post-CAS callback is not used for DV work. The important boundary
is earlier: complete DV work must precede the existing pending transition, and
CID assignment plus footprint registration must not expose an unknown lower-CID
gap to a later transaction.

During migration, compatibility methods may keep the old public names, but they
must stage fragments only. The old `_val_tree`, `_multi_val_state`, direct
per-row `insert`/`remove`, and unused transaction implementation must be removed
or made unreachable; retaining them as a fallback would leave B4, C5, C6, D1,
and E6 open in the deployed system.
