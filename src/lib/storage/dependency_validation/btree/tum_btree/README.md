# TUM `btree-cpp` takeover boundary

This directory contains DV-Tree's owned, adapted copy of the basic variable-key
B+ tree from the TUM `btree-cpp` research implementation. The original
`btree-cpp` working tree is not modified or linked at build time. DV-Tree builds
this copy directly so its structural behavior can be tested and evolved without
maintaining a runtime dependency on the research project.

## Structural mechanisms retained

The takeover preserves the mechanisms that motivated using `btree-cpp` as the
page-engine foundation:

- fixed 4 KiB slotted pages;
- lower and upper fence keys with prefix truncation;
- four-byte post-prefix key heads;
- 16 sampled search hints;
- variable-length key compaction;
- separator selection and split mechanics;
- a fixed-size pointer payload per leaf slot.

Dense, hash/fingerprint, adaptive, and head-only inner-node alternatives are not
part of this copy. DV-Tree uses one basic, general variable-key layout rather
than combining mutually alternative experimental layouts.

## DV-Tree-owned adaptations

The code in this directory is not an untouched upstream snapshot. DV-Tree owns
and tests the following integration and concurrency changes:

- `NodeControl` is separated from the copyable 4 KiB `PageBody`;
- readers consume immutable published page images;
- node versions and write state implement optimistic lock coupling with restart;
- child-version coupling and stale fence-prefix observations restart safely;
- leaf pages have stable bidirectional sibling links;
- splits validated-try-lock the previous leaf before relinking;
- root publication and page-image publication use atomic visibility boundaries;
- per-tree atomic inner/leaf node counters support concurrent memory accounting
  without traversing mutable structural links;
- oversized and duplicate keys use checked exceptions instead of abort-only
  upstream assumptions;
- merges and node reclamation remain disabled, preserving pointer lifetime.

The `BTreeCppAdapter` one directory above is the boundary between this structural
engine and DV-Tree. Key normalization, `DependencyEntry`, FD/OD semantics, MVCC
history, commit tickets, CID ordering, and publication revalidation are DV-Tree
components and are not inherited from TUM `btree-cpp`.

The upstream research artifact's licensing and redistribution terms still need
to be confirmed before shipping this adapted copy outside the research project.
