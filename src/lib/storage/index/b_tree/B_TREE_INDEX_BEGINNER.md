# B-Tree Index: A Beginner's Guide

This guide explains the B-Tree index implementation in Hyrise for readers new to database internals.

## What is This Index For?

This B-Tree index helps verify **data dependencies** efficiently:

- **Functional Dependency (FD):** If two rows have the same value in column A, they must have the same value in column B. Example: `employee_id → department` means each employee belongs to exactly one department.

- **Order Dependency (OD):** If row 1 comes before row 2 in column A, it should also come before in column B. Example: `hire_date ~> seniority_level` means earlier hires have higher seniority.

## Why Use a B-Tree?

B-Trees keep data sorted, making it efficient to:
1. Find all rows with a specific key value
2. Check neighboring key values for violations
3. Update violation counts incrementally (not from scratch)

## Core Concepts

### The Tree Structure

```
         [Root: 5, 10]
        /      |      \
    [2,3]   [7,8]   [12,15]   ← Internal nodes
   /  |  \   ...      ...
[1] [2] [3]                   ← Leaf nodes (linked together)
```

- Each node holds multiple keys
- Leaf nodes store the actual data pointers
- Leaf nodes are linked left-to-right for neighbor access

### Metadata Per Key

For each unique key value, we track:

```
Key: 5
├── start_index: 100    # Where this key's rows start in the offset array
├── count: 3            # How many rows have this key
├── right_neighbor_flag: 0 or 1  # Does boundary with key 6 violate dependency?
└── local_violations: 2 # count - 1 = potential violations within this key
```

### Global Violation Counter

Instead of scanning all data, we maintain one number:

```
global_violation_count = sum of all flags + sum of all local violations
```

- If 0: No violations found, dependency likely holds
- If > 0: Some violations exist

## How Validation Works

### Online (Incremental) Validation

The key feature of this index is **online validation** - you don't need to rebuild the index or scan the entire table when data changes. Instead:

1. When a new row is inserted → call `insert_entry_for_validation()`
2. When a row is deleted → call `delete_entry_for_validation()`  
3. Check `global_violation_count == 0` → **O(1) dependency check!**

The index automatically handles new key values (they're added to the tree) and key removal (when the last row with a key is deleted, the key is removed).

### On Insert

When adding a new row:

1. Find where the key belongs in the tree
2. If it's now the smallest key in its group, check the left neighbor
3. If it's the largest, check the right neighbor  
4. Update the local violation count (more duplicates = more potential violations)
5. Update the global counter

### On Delete

When removing a row:

1. Find the key in the tree
2. If we're removing a boundary value, update neighbor flags
3. Decrease the local violation count
4. Update the global counter

### On Update

An update is just: delete old + insert new.

## MVCC: Handling Concurrent Transactions

### The Problem

Multiple users may read and write simultaneously. How do we ensure consistency?

### The Solution: Snapshots

Each transaction gets a "snapshot" of the data at a specific point in time.

```
Transaction T1 (snapshot at time 10):
- Sees all rows committed before time 10
- Doesn't see T2's uncommitted changes

Transaction T2 (snapshot at time 12):
- Sees rows committed before time 12
- Can see T1's changes if T1 committed before time 12
```

### Row Visibility Rules

Each row has:
- `begin_cid`: When it became visible (commit time)
- `end_cid`: When it was deleted (MAX if still active)

A row is visible to snapshot S if: `begin_cid <= S < end_cid`

### Handling Conflicts

When two transactions try to modify the same row:

```
T1: Locks row 5 for update
T2: Tries to lock row 5 → BLOCKED or CONFLICT
T1: Commits
T2: Can now proceed (or retry if aborted)
```

## Test Scenarios Explained

### Basic Tests

| Test | What It Checks |
|------|----------------|
| FD with no violations | `A=1` always has `B=10` → no violations |
| FD with violations | `A=1` has `B=10` and `B=20` → violation! |

### MVCC Tests

| Test | Scenario |
|------|----------|
| Transaction Isolation | T2's uncommitted changes are invisible to T1 |
| Deleted Rows | Once deleted and committed, rows disappear |
| Phantom Prevention | New rows don't suddenly appear mid-transaction |
| Write Conflicts | Can't modify a row another transaction is modifying |

### Edge Cases

| Test | Scenario |
|------|----------|
| Single Entry | Index with just one key |
| All NULLs | NULLs are tracked separately, not indexed |
| 100 Duplicates | Local violation count = 99 |

## Code Walkthrough

### Creating an Index

```cpp
// Index column A of a table
auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
auto index = std::make_shared<BTreeIndex>({segment});
```

### Checking for Violations

```cpp
// Define what a violation looks like
auto check_violation = [&](const auto& key1, const auto& key2) {
  // Look up B values for these A values
  // Return true if they should be equal but aren't
  return b_value_for_key1 != b_value_for_key2;
};

// Insert and track
auto deltas = index->insert_entry_for_validation({a_value}, {b_value}, check_violation);

// Check result
if (index->global_violation_count == 0) {
  std::cout << "FD A → B holds!" << std::endl;
} else {
  std::cout << "FD violated " << index->global_violation_count << " times" << std::endl;
}
```

### Simulating MVCC Visibility

```cpp
// Create table with MVCC
auto table = Table::create_dummy_table({{"A", DataType::Int}, {"B", DataType::Int}});

// Get MVCC data for a chunk
auto mvcc_data = table->get_chunk(ChunkID{0})->mvcc_data();

// Set visibility: row 0 committed at time 5
mvcc_data->set_begin_cid(ChunkOffset{0}, CommitID{5});
mvcc_data->set_end_cid(ChunkOffset{0}, MAX_COMMIT_ID);  // Still active

// Check visibility for snapshot at time 10
bool visible = begin_cid <= 10 && 10 < end_cid;  // true
```

## Common Patterns

### Pattern 1: Check Before Commit

```cpp
auto context = TransactionManager::get().new_transaction_context();
// ... make changes ...

if (index->global_violation_count > 0) {
  context->rollback(RollbackReason::User);  // Abort due to violation
} else {
  context->commit();
}
```

### Pattern 2: Incremental Validation

```cpp
// Don't recompute everything - use deltas
for (const auto& change : changes) {
  auto delta = index->insert_entry_for_validation(...);
  // global_violation_count already updated
}
// Just check the final count
```

### Pattern 3: Neighbor Flag Updates

```cpp
// When inserting the smallest key, the left neighbor's flag may change
// because it now has a different right neighbor

// Before: [3] → [5, 7]    (3's right neighbor is 5)
// After:  [3] → [4, 5, 7] (3's right neighbor is 4)
// Need to recompute flag between 3 and 4
```

## Summary

1. **B-Tree structure** keeps keys sorted for efficient neighbor access
2. **Per-key metadata** tracks local violations and neighbor flags
3. **Global counter** gives O(1) violation check
4. **Delta updates** avoid full rescans
5. **MVCC integration** ensures correct validation under concurrency

## Next Steps

- Read the full API documentation in `B_TREE_INDEX.md`
- Study the test file `b_tree_index_test.cpp` for more examples
- Look at how Hyrise uses MVCC in `src/lib/storage/mvcc_data.hpp`
