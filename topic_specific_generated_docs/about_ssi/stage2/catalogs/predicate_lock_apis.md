# SSI Predicate Lock API Catalog

## Core Lock Acquisition Functions

### 1. PredicateLockRelation()

**Source**: `./src/backend/storage/lmgr/predicate.c:1850`

```c
void PredicateLockRelation(Relation relation, Snapshot snapshot)
```

**Caller Context**:
- Sequential scans of entire table
- Full table imports/copies
- Bulk operations without WHERE clause
- DDL operations (CLUSTER, etc.)

**Lock Granularity**: Relation-level (coarsest)

**Example**:
```c
// In heapam.c: sequential scan
SeqNext(...) {
    if (SerializationNeededForRead(relation, snapshot))
        PredicateLockRelation(relation, snapshot);
    // Process heap...
}
```

**Error Conditions**:
- Out of predicate lock memory: `ERROR`
- Relation not eligible (temp/system): No error, quick return

---

### 2. PredicateLockPage()

**Source**: `./src/backend/storage/lmgr/predicate.c:1880`

```c
void PredicateLockPage(
    Relation relation,
    BlockNumber blkno,
    Snapshot snapshot)
```

**Caller Context**:
- Page-level access patterns
- Scans across multiple pages
- Index leaf pages
- Page pruning operations

**Lock Granularity**: Page-level (medium)

**Promotion Rules**:
- If relation lock exists: redundant, return
- If page locks on same relation > threshold: promote to relation lock
- If single transaction has > max_predicate_locks_per_transaction: force promotion

**Example**:
```c
// In nbtree.c: index scan
_bt_next(...) {
    if (check_predicate_locks)
        PredicateLockPage(relation, current_page, snapshot);
    // Read page...
}
```

---

### 3. PredicateLockTID()

**Source**: `./src/backend/storage/lmgr/predicate.c:1905`

```c
void PredicateLockTID(
    Relation relation,
    ItemPointer tid,
    Snapshot snapshot,
    TransactionId tuple_xid)
```

**Caller Context**:
- Tuple-level visibility checks (most common)
- Sequential scans (one lock per visible tuple)
- Index scans retrieving specific tuples
- Bitmap scans

**Lock Granularity**: Tuple-level (finest)

**Parameters**:
- `tuple_xid`: XID that wrote the tuple (for optimization)

**Skip Conditions**:
- `tuple_xid < snapshot->xmin`: Skip (too old to conflict)
- Coarser lock already covers target
- Already reached promotion threshold

**Example**:
```c
// In heapam.c visibility checks  
HeapFetchTuple(...) {
    if (tuple_visible && SERIALIZABLE_ISOLATION())
        PredicateLockTID(relation, &tid, snapshot, xmin_id);
    return tuple;
}
```

---

## Lock Transfer Functions

### 4. PredicateLockPageSplit()

**Source**: `./src/backend/storage/lmgr/predicate.c:2420`

```c
void PredicateLockPageSplit(
    Relation relation,
    BlockNumber oldblkno,
    BlockNumber newblkno)
```

**When Called**: B-tree page split operations

**Behavior**: Keeps lock on old page (conservative approach)

**Rationale**: After split, old page still represents portion of predicate range

---

### 5. PredicateLockPageCombine()

**Source**: `./src/backend/storage/lmgr/predicate.c:2460`

```c
void PredicateLockPageCombine(
    Relation relation,
    BlockNumber oldblkno,
    BlockNumber newblkno)
```

**When Called**: B-tree page combine (merge) operations

**Behavior**: Union of locks from both pages stays on combined page

---

### 6. TransferPredicateLocksToHeapRelation()

**Source**: `./src/backend/storage/lmgr/predicate.c:2490`

```c
void TransferPredicateLocksToHeapRelation(Relation relation)
```

**When Called**:
- REINDEX command
- Automatic index rebuild
- Vacuum (if necessary)

**Effect**: All locks on index pages → locks on heap relation

**Rationale**: Index rebuilt, physical structure changed, logical data unchanged

---

### 7. TransferPredicateLocksToNewTarget()

**Source**: `./src/backend/storage/lmgr/predicate.c:2150`

```c
bool TransferPredicateLocksToNewTarget(
    PREDICATELOCKTARGETTAG oldtargettag,
    PREDICATELOCKTARGETTAG newtargettag,
    bool removeOld)
```

**Purpose**: Internal function for complex lock transfers

**Returns**:
- `true`: Transfer completed successfully
- `false`: Failed (shouldn't happen in production)

**Parameters**:
- `removeOld`: If true, delete old target after transfer

---

## Conflict Detection Functions

### 8. CheckForSerializableConflictOut()

**Source**: `./src/backend/storage/lmgr/predicate.c:1435`

```c
void CheckForSerializableConflictOut(
    Relation relation,
    TransactionId xid,
    Snapshot snapshot)
```

**When Called**: When reading tuple written by concurrent transaction

**Preconditions**:
- Tuple is visible in snapshot
- Tuple xmin > snapshot->xmin (concurrent writer)
- Tuple xid is serializable transaction

**Effect**:
- Looks up writing transaction
- Creates rw-conflict if found
- Triggers dangerous structure check

**Called From**: Heap/index scan tuple visibility checks

---

### 9. CheckForSerializableConflictIn()

**Source**: `./src/backend/storage/lmgr/predicate.c:1512`

```c
void CheckForSerializableConflictIn(
    Relation relation,
    ItemPointer tid,
    BlockNumber blkno)
```

**When Called**: Before INSERT/UPDATE/DELETE of tuple

**Preconditions**:
- Transaction is serializable
- Relation participates in predicate locking
- Write is about to happen

**Effect**:
- Checks all three lock granularities (relation, page, tuple)
- For each lock found, creates rw-conflict
- Triggers dangerous structure detection

**Called From**:
- heap_insert()
- heap_update()
- heap_delete()
- heap_multi_insert()

---

### 10. CheckTableForSerializableConflictIn()

**Source**: `./src/backend/storage/lmgr/predicate.c:1560`

```c
void CheckTableForSerializableConflictIn(Relation relation)
```

**When Called**: Bulk table operations

**Equivalent To**: CheckForSerializableConflictIn() for entire table

**Called From**:
- TRUNCATE command
- CLUSTER command
- VACUUM (in some cases)

---

## Commit-Time Validation

### 11. PreCommit_CheckForSerializationFailure()

**Source**: `./src/backend/storage/lmgr/predicate.c:1315`

```c
void PreCommit_CheckForSerializationFailure(void)
```

**When Called**: In CommitTransaction() before actual commit

**Effect**:
- Final dangerous structure search
- May mark transaction as DOOMED
- May raise SERIALIZATION_FAILURE error

**Synchronization**: May hold SerializableFinishedListLock briefly

---

## Lock Release

### 12. ReleasePredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:1224`

```c
void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe)
```

**When Called**:
- At transaction end (commit or abort)
- When read-only transaction becomes safe

**Parameters**:
- `isCommit`: true for commit, false for abort
- `isReadOnlySafe`: true if RO-safe determined

**Effect**:
- For abort: Immediate cleanup
- For commit: Locks kept for overlap tracking
- For RO-safe: Locks released early

---

## 2PC Functions

### 13. AtPrepare_PredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:2540`

```c
void AtPrepare_PredicateLocks(void)
```

**When Called**: During PREPARE phase of 2PC

**Effect**: Predicate locks serialized to 2PC state file

---

### 14. PostPrepare_PredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:2570`

```c
void PostPrepare_PredicateLocks(TransactionId xid)
```

**When Called**: After XID assigned to prepared transaction

**Effect**: Local state cleaned, record kept in shared memory

---

### 15. PredicateLockTwoPhaseFinish()

**Source**: `./src/backend/storage/lmgr/predicate.c:2600`

```c
void PredicateLockTwoPhaseFinish(
    TransactionId xid,
    bool isCommit)
```

**When Called**: At COMMIT PREPARED or ROLLBACK PREPARED

**Effect**:
- If commit: Move to finished list
- If abort: Immediate cleanup

---

### 16. predicatelock_twophase_recover()

**Source**: `./src/backend/storage/lmgr/predicate.c:2630`

```c
void predicatelock_twophase_recover(
    TransactionId xid,
    uint16 info,
    void *recdata,
    uint32 len)
```

**When Called**: During crash recovery, processing 2PC WAL records

**Effect**: Reconstructs predicate locks from WAL

---

## Initialization Functions

### 17. InitPredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:170`

```c
void InitPredicateLocks(void)
```

**When Called**: Postmaster startup (per backend)

**Effect**:
- Initializes local predicate lock hash
- Attaches to shared memory structures
- Sets up backends local state

---

### 18. PredicateLockShmemSize()

**Source**: `./src/backend/storage/lmgr/predicate.c:140`

```c
Size PredicateLockShmemSize(void)
```

**Purpose**: Calculate shared memory needed

**Called From**: ipci.c during startup

---

## Utility Functions

### 19. RegisterPredicateLockingXid()

**Source**: `./src/backend/storage/lmgr/predicate.c:840`

```c
void RegisterPredicateLockingXid(void)
```

**Purpose**: Register transaction XID mapping

**Called From**: GetSerializableTransactionSnapshotInt()

---

### 20. GetSerializableTransactionSnapshot()

**Source**: `./src/backend/storage/lmgr/predicate.c:310`

```c
Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot)
```

**Purpose**: Entry point for serializable transactions

**Called From**: snapmgr.c GetTransactionSnapshot()

---

### 21. SetSerializableTransactionSnapshot()

**Source**: `./src/backend/storage/lmgr/predicate.c:385`

```c
void SetSerializableTransactionSnapshot(
    Snapshot snapshot,
    VirtualTransactionId *sourcevxid,
    int sourcepid)
```

**Purpose**: Parallel worker snapshot setup

**Called From**: parallel.c parallel worker initialization

---

### 22. GetPredicateLockStatusData()

**Source**: `./src/backend/storage/lmgr/predicate.c:3000`

```c
PredicateLockData *GetPredicateLockStatusData(void)
```

**Purpose**: Export locks for pg_locks view

**Called From**: lockfuncs.c

---

## API Summary by Use Case

### Sequential Scan
```
PredicateLockRelation(relation, snapshot)
```

### Index Scan (Multiple Tuples)
```
For each leaf page:
    PredicateLockPage(relation, page, snapshot)
For each visible tuple:
    PredicateLockTID(relation, tid, snapshot, xid)
```

### Single Row Lookup
```
PredicateLockTID(relation, tid, snapshot, xid)
```

### Insert/Update/Delete
```
CheckForSerializableConflictIn(relation, tid, blkno)
```

### Bulk Operation
```
CheckTableForSerializableConflictIn(relation)
```

### Transaction End
```
if (isReadOnlySafe):
    ReleasePredicateLocks(false, true)
else:
    ReleasePredicateLocks(isCommit, false)
```

### Prepare Transaction
```
AtPrepare_PredicateLocks()
PostPrepare_PredicateLocks(xid)
```

### Finish 2PC
```
PredicateLockTwoPhaseFinish(xid, isCommit)
```

### Crash Recovery
```
predicatelock_twophase_recover(xid, info, recdata, len)
```

---

## Performance Characteristics

| Function | Time | Lock Held |
|----------|------|-----------|
| PredicateLockRelation | O(1) | partition |
| PredicateLockPage | O(m) | partition |
| PredicateLockTID | O(m) | partition |
| CheckForSerializableConflictOut | O(1) | partition |
| CheckForSerializableConflictIn | O(n) | partition |
| PreCommit_CheckForSerializationFailure | O(d) | finished-list |
| ReleasePredicateLocks | O(k) | predicate-list |

Where:
- m = locks on target
- n = locks on same relation/page
- k = locks held by transaction
- d = depth of conflict search

