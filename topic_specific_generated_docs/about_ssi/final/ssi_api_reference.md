# SSI API Reference

**Function signatures organized by subsystem with descriptions and usage patterns.**

---

## Transaction Lifecycle API

### GetSerializableTransactionSnapshot()

```c
Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot);
```

**Purpose**: Primary entry point for obtaining a snapshot for SERIALIZABLE transactions.

**Parameters**:
- `snapshot`: Input snapshot (typically from snapmgr)

**Returns**: 
- `Snapshot`: Modified snapshot with SSI metadata

**Called By**: `snapmgr.c:GetTransactionSnapshot()`

**Side Effects**: 
- Creates/updates SERIALIZABLEXACT in shared memory
- May trigger SERIALIZATION_FAILURE if conflict detected

**Example**:
```c
// In xact.c
if (IsolationLevel == SERIALIZABLE) {
    snapshot = GetSerializableTransactionSnapshot(snapshot);
    MySerializableXact = snapshot->ssi_xact;  // Cache for later use
}
```

**Performance**: O(1), brief lock hold on `SerializableXactHashLock`

---

### PreCommit_CheckForSerializationFailure()

```c
void PreCommit_CheckForSerializationFailure(void);
```

**Purpose**: Performs final serialization validation before commit.

**Parameters**: None (uses global `MySerializableXact`)

**Returns**: Void (raises exception on failure)

**Exceptions**:
- `ERROR: SERIALIZATION_FAILURE` (SQLSTATE 40001) if dangerous structure detected

**Called By**: `xact.c:CommitTransaction()`

**Side Effects**: 
- May mark transaction as DOOMED
- May set `MySerializableXact` flags

**Example**:
```c
// In xact.c CommitTransaction()
if (TransactionBlockStateIs(TRANS_COMMIT)) {
    PreCommit_CheckForSerializationFailure();  // May raise
    // If we get here, commit is safe
    ProceedWithPhysicalCommit();
}
```

**Performance**: 
- RO_SAFE txns: O(1)
- Write txns: O(E) where E = conflict edges, typical O(1)

---

### ReleasePredicateLocks()

```c
void ReleasePredicateLocks(bool isCommit, bool isParallel);
```

**Purpose**: Release all predicate locks held by current transaction.

**Parameters**:
- `isCommit`: true if transaction committed, false if aborted
- `isParallel`: true if transaction is parallel query worker

**Returns**: Void

**Called By**: `xact.c:CommitTransaction()`, `xact.c:AbortTransaction()`

**Side Effects**:
- Removes locks from hash tables
- Frees SERIALIZABLEXACT (if last reference)
- Updates finished transaction list

**Example**:
```c
// In xact.c
if (status == TRANS_ABORT) {
    ReleasePredicateLocks(false, false);  // False: aborted
} else if (status == TRANS_COMMIT) {
    ReleasePredicateLocks(true, false);   // True: committed
}
```

**Performance**: O(n) where n = txn's lock count, typically <100

---

## Predicate Lock Acquisition API

### PredicateLockRelation()

```c
void PredicateLockRelation(Relation relation, Snapshot snapshot);
```

**Purpose**: Acquire relation-level predicate lock (entire table).

**Parameters**:
- `relation`: Relation being scanned
- `snapshot`: Current snapshot (for visibility checks)

**Returns**: Void

**Called By**: Sequential scans, full table operations

**Side Effects**:
- Creates PREDICATELOCK entry
- May trigger coalescing if memory exceeded

**Example**:
```c
// In heapam.c SeqNext()
PredicateLockRelation(relation, snapshot);
```

**Performance**: O(1) hash lookup + insertion

**When to Use**: 
- Full table scans without WHERE clause
- Sequential table copies
- Operations on entire relation

---

### PredicateLockPage()

```c
void PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot);
```

**Purpose**: Acquire page-level predicate lock (8KB page).

**Parameters**:
- `relation`: Relation containing page
- `blkno`: Page block number
- `snapshot`: Current snapshot

**Returns**: Void

**Called By**: Index scans, partial table scans

**Side Effects**: Creates PREDICATELOCK entry, may trigger promotion

**Example**:
```c
// In nbtree.c index scan
PredicateLockPage(rel, BlockNumberOfPage, snapshot);
```

**Performance**: O(1) typical, O(n) if coalescing triggered

---

### PredicateLockTuple()

```c
void PredicateLockTuple(Relation relation, HeapTuple tuple, Snapshot snapshot);
```

**Purpose**: Acquire tuple-level predicate lock (specific row).

**Parameters**:
- `relation`: Relation containing tuple
- `tuple`: Tuple being accessed
- `snapshot`: Current snapshot

**Returns**: Void

**Called By**: Executor during tuple visibility checks

**Side Effects**: Creates PREDICATELOCK entry, may coalesce

**Example**:
```c
// In executor
if (HeapTupleSatisfiesMVCC(...)) {
    PredicateLockTuple(relation, tuple, snapshot);
    // Process tuple...
}
```

**Performance**: O(1) typical, O(n) if memory pressure

---

## Conflict Detection API

### CheckForSerializableConflictOut()

```c
void CheckForSerializableConflictOut(bool visible, Relation relation, 
                                     HeapTuple tuple, Buffer buffer,
                                     Snapshot snapshot);
```

**Purpose**: Check for conflicts when reading data (reader acquiring predicate lock).

**Parameters**:
- `visible`: Whether tuple is visible to this transaction
- `relation`: Relation containing tuple
- `tuple`: Tuple being read
- `buffer`: Buffer containing tuple
- `snapshot`: Current snapshot

**Returns**: Void

**Called By**: Executor during heap scan, visibility check

**Side Effects**: 
- May create predicate lock
- May create RWConflict edges
- May trigger dangerous structure detection (non-fatal)

**Example**:
```c
// In executor after visibility check
if (HeapTupleSatisfiesMVCC(...)) {
    CheckForSerializableConflictOut(true, relation, tuple, buffer, snapshot);
}
```

**Performance**: O(n) where n = txns holding conflicting locks, typical O(1-5)

---

### CheckForSerializableConflictIn()

```c
void CheckForSerializableConflictIn(Relation relation, HeapTuple tuple,
                                    Buffer buffer);
```

**Purpose**: Check for conflicts when writing data (writer detecting reader locks).

**Parameters**:
- `relation`: Relation being modified
- `tuple`: Tuple being written (for update/delete)
- `buffer`: Buffer containing tuple

**Returns**: Void

**Called By**: Executor during insert/update/delete, before WAL

**Side Effects**:
- May find conflicts with prior readers
- May create RWConflict edges
- May detect dangerous structures

**Example**:
```c
// In heapam.c before writing
CheckForSerializableConflictIn(relation, tuple, buffer);
heapam_insert(relation, tuple, ...);  // Proceed if no conflict
```

**Performance**: O(n) where n = txns with overlapping read locks, typical O(1-5)

---

## Lock Acquisition Support API

### PredicateLockAcquire()

```c
static void PredicateLockAcquire(PREDICATELOCKTAG *tag,
                                 bool insert);
```

**Purpose**: Internal function for core lock acquisition with promotion heuristic.

**Parameters**:
- `tag`: Lock target tag (relation/page/tid)
- `insert`: Whether to allocate new lock if not found

**Returns**: Void

**Called By**: PredicateLockRelation/Page/Tuple()

**Side Effects**: May trigger coalescing on memory pressure

**Note**: Usually not called directly; use PredicateLock* wrappers instead.

---

## Memory Management API

### InitPredicateLocks()

```c
void InitPredicateLocks(void);
```

**Purpose**: Initialize predicate locking subsystem at server startup.

**Parameters**: None

**Returns**: Void

**Called By**: `PostmasterMain()` during server startup

**Side Effects**: 
- Allocates shared memory for SSI structures
- Initializes hash tables
- Initializes LWLocks

**Example**:
```c
// In postmaster.c
InitPredicateLocks();  // After shared memory initialized
```

**Performance**: O(1), runs once per server restart

---

### SummarizeOldestCommittedSxact()

```c
static void SummarizeOldestCommittedSxact(void);
```

**Purpose**: Move old completed transactions to SLRU, freeing shared memory.

**Parameters**: None

**Returns**: Void

**Called By**: Periodic maintenance (approximately 1-2 seconds)

**Side Effects**: 
- Writes to SLRU (pg_serial file)
- Removes from FinishedSerializableTransactions list
- Frees memory

**Performance**: O(n) where n = txns to summarize, typical O(100-1000)

---

## Diagnostic / Utility API

### OnConflict_CheckForSerializationFailure()

```c
static void OnConflict_CheckForSerializationFailure(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer);
```

**Purpose**: Main dangerous structure detection algorithm (called during conflict detection).

**Parameters**:
- `reader`: Transaction that has read lock
- `writer`: Transaction about to write (or has written)

**Returns**: Void

**Side Effects**: May set SXACT_FLAG_DOOMED on one transaction

**Note**: Called from CheckForSerializableConflictIn()

**Performance**: O(C) where C = conflict graph complexity, typical O(1-10)

---

## Header File API

Functions declared in `predicate.h` (public API):

```c
void InitPredicateLocks(void);
Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot);
bool PhantomPredicateLockUpgradeCheck(Relation relation, Snapshot snapshot);
void PredicateLockRelation(Relation relation, Snapshot snapshot);
void PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot);
void PredicateLockTuple(Relation relation, HeapTuple tuple, Snapshot snapshot);
void CheckForSerializableConflictOut(bool visible, Relation relation, 
                                     HeapTuple tuple, Buffer buffer,
                                     Snapshot snapshot);
void CheckForSerializableConflictIn(Relation relation, HeapTuple tuple,
                                    Buffer buffer);
void ReleasePredicateLocks(bool isCommit, bool isParallel);
void PreCommit_CheckForSerializationFailure(void);
```

---

## Common Usage Patterns

### Pattern 1: Simple Read-Only Transaction
```c
Snapshot snap = GetSerializableTransactionSnapshot(current_snapshot);
// scan table
PredicateLockRelation(rel, snap);
ScanHeap(rel, snap);  // Will call CheckForSerializableConflictOut internally
```

### Pattern 2: Write Transaction With Conflict Check
```c
Snapshot snap = GetSerializableTransactionSnapshot(current_snapshot);
PredicateLockRelation(rel, snap);
CheckForSerializableConflictIn(rel, tuple, buf);  // Check before write
heap_update(rel, tuple, ...);  // Perform write
```

### Pattern 3: Commit With Validation
```c
PreCommit_CheckForSerializationFailure();  // May raise SERIALIZATION_FAILURE
MarkTransactionCommitted();
ReleasePredicateLocks(true, false);
```

---

## See Also

- [Predicate Lock APIs Catalog](15_catalog_predicate_lock_apis.md) - Detailed API documentation
- [Conflict and Commit APIs](16_catalog_conflict_and_commit_apis.md) - Additional functions
- [Source Map](appendix_source_map.md) - File locations and line numbers
- [Symbol Index](appendix_symbol_index.md) - Alphabetical reference
