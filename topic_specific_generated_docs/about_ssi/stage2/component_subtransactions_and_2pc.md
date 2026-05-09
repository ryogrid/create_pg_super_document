# PostgreSQL SSI: Subtransactions, Two-Phase Commit, and Advanced Topics

## Part 1: Subtransactions

### Subtransaction Handling in SSI

Subtransactions (SAVEPOINTs) in SSI use the parent transaction's SERIALIZABLEXACT record:

```c
// In backend/access/transam/xact.c
StartSubTransaction() {
    // NEW subtransaction level
    // But SAME SERIALIZABLEXACT as parent!
    
    // Why: to track all writes at top-level xid
    // Subtransaction rollback won't affect predicate locks
}
```

#### Key Invariants

```
1. All predicate locks use parent xid
2. Subtransaction xid < parent xid
3. Rollback of subtransaction doesn't release locks
4. Only parent commit/abort affects locks
```

#### SubTransGetTopmostTransaction()

```c
TransactionId SubTransGetTopmostTransaction(TransactionId xid) {
    // Recursively climb to top-level transaction
    // Used to map any xid → parent SERIALIZABLEXACT
    
    if (xid is top-level):
        return xid
    else:
        return SubTransGetTopmostTransaction(parent(xid))
}
```

### Subtransaction Abort Semantics

```c
// When SAVEPOINT name ROLLBACK is executed:

AbortSubTransaction() {
    // Release subtransaction-local state
    // But NOT predicate locks!
    
    // Predicate locks are recorded by:
    // (top-level-xid, target) pair
    // Not by subtransaction xid
}

ReleasePredicateLocks() {
    // Called only for TOP-LEVEL commit/abort
    // NOT for subtransaction abort
}
```

### MultiXact Handling in Subtransactions

```c
// Locks held by subtransaction are part of MultiXact
// When subtransaction aborts, removed from MultiXact
// But SSI tracks top-level xid anyway
```

---

## Part 2: Two-Phase Commit (2PC)

### 2PC Predicate Lock Persistence

**Key Challenge**: Predicate locks must survive PREPARE phase and be recoverable on crash.

#### AtPrepare_PredicateLocks() - Prepare Phase

**Source**: `./src/backend/storage/lmgr/predicate.c`

```c
void AtPrepare_PredicateLocks(void) {
    
    // Called during PrepareTransaction()
    
    // Write to 2PC state file:
    // 1. Transaction record (xmin, flags)
    // 2. All predicate locks
    
    // Serialization format:
    TwoPhasePredicateXactRecord {
        TransactionId xmin;
        uint32 flags;
    }
    
    TwoPhasePredicateLockRecord {
        PREDICATELOCKTARGETTAG target;
        uint16 filler;
    }
}
```

#### PostPrepare_PredicateLocks() - Post-Prepare

```c
void PostPrepare_PredicateLocks(TransactionId xid) {
    
    // Called after transaction XID is assigned
    // 
    // Mark transaction as PREPARED (not COMMITTED)
    // prepareSeqNo = LastSxactCommitSeqNo++
    
    // Keep SERIALIZABLEXACT record alive
    // Predicate locks still held
}
```

#### PredicateLockTwoPhaseFinish() - Commit/Abort

**Signature**:
```c
void PredicateLockTwoPhaseFinish(TransactionId xid, bool isCommit)
```

**Implementation**:
```c
void PredicateLockTwoPhaseFinish(TransactionId xid, bool isCommit) {
    
    sxact = GetSerializableTransactionByXid(xid);
    
    if (isCommit) {
        // Move to finished list
        Mark as COMMITTED
        Set commitSeqNo
        SerialAdd() - record to SLRU
        
        // Keep locks for overlap tracking
        // Later released by ClearOldPredicateLocks()
    } else {
        // Abort: immediate cleanup
        ReleaseAllPredicateLocks(sxact)
        ReleasePredXact(sxact)
    }
}
```

### predicatelock_twophase_recover() - Recovery

**Signature**:
```c
void predicatelock_twophase_recover(
    TransactionId xid,
    uint16 info,
    void *recdata,
    uint32 len)
```

**Purpose**: Recover 2PC transaction and locks from WAL after crash.

**Workflow**:
```
1. Read transaction record from WAL
2. Reconstruct SERIALIZABLEXACT
3. Read predicate lock records
4. Recreate PREDICATELOCK entries
5. Re-establish conflict graph if needed
```

#### Conflict Graph Recovery

When recovering prepared transaction that had conflicts:

```c
// Look up any transactions that:
// 1. Committed after PREPARE
// 2. Had conflicts with recovered transaction

for each committed xid after prepared xid:
    if (ConflictExists(recovered_txn, committed_xid)):
        // Re-establish conflict graph edge
        FlagRWConflict(reader, writer)
        // May detect dangerous structure!
```

---

## Part 3: Concurrency and Shared Memory

### Lock Ordering (Critical for Deadlock Prevention)

**Must acquire in this order**:

```
1. SerializableFinishedListLock
2. SerializablePredicateListLock
3. SERIALIZABLEXACT's perXactPredicateListLock (parallel only)
4. PredicateLockHashPartitionLock (in ascending hash order)
5. SerializableXactHashLock
6. SerialControlLock
7. SLRU bank locks (per SimpleLru)
```

**Release in reverse order** (LIFO)

### Partition Locking Strategy

```c
#define NUM_PREDICATE_LOCK_PARTITIONS 16

// Hash-based partition assignment
partition_id = hash(target) % NUM_PREDICATE_LOCK_PARTITIONS

// Multiple transactions can lock different partitions simultaneously
// Reduces contention on predicate lock hash table
```

### Parallel Query Worker Synchronization

```c
// Parent and worker share same SERIALIZABLEXACT
// Synchronization:
// 1. Parent locks: SerializablePredicateListLock (shared)
// 2. Worker locks: sxact->perXactPredicateListLock (per-worker)
// 3. Both protect: predicateLocks list

// Purpose: Prevent concurrent modification of lock list
// during GC or conflict detection
```

#### Parallel Query Cleanup

```
Leader (parent):
├── Main transaction processing
├── Submit parallel workers
└── After workers finish:
    ├── ReleaseOneSerializableXact(partial=true)
    │   └── Mark as PARTIALLY_RELEASED
    └── At END-OF-TRANSACTION:
        └── ReleaseOneSerializableXact(partial=false)
            └── Final cleanup

Workers:
└── Share leader's SERIALIZABLEXACT
    └── Can't independently cleanup
```

---

## Part 4: Read-Only Optimization (Detailed)

### GetSafeSnapshot() Mechanism

Already covered in snapshot component, but key points for integration:

```c
bool IsSafeSnapshot(SERIALIZABLEXACT *roXact) {
    
    // Snapshot is safe if:
    // 1. No active R/W transactions
    for each sxact in activeList:
        if (!SxactIsReadOnly(sxact)):
            if (sxact->xmin < roXact->xmin):
                return false  // Overlapping R/W
    
    // 2. All prior R/W transactions committed without conflicts
    for each conflict in roXact->possibleUnsafeConflicts:
        if (!SxactIsCommitted(conflict->sxactOut)):
            return false
        if (conflict flags indicate unsafe):
            return false
    
    return true
}
```

### DEFERRABLE Transaction Handling

```sql
SET TRANSACTION DEFERRABLE;
-- Blocks in GetSafeSnapshot()
-- Waits for safe snapshot to become available
-- Unblocks when snapshot is safe
-- Then proceeds with guaranteed no-serialization-failures
```

### SQL Level Control

```c
// In backend/utils/misc/postgresql.conf.sample

# Enable safe snapshots for read-only
enable_deferrable_transactions = on  # (not actually in GUCs)
```

---

## Part 5: Observability and Debugging

### GetPredicateLockStatusData() - Status Export

**Signature**:
```c
PredicateLockData *GetPredicateLockStatusData(void)
```

**Purpose**: Export predicate lock information for `pg_locks` view and monitoring.

**Data Structure**:
```c
typedef struct PredicateLockData {
    int nelements;
    PREDICATELOCKTARGETTAG *locks;
    SERIALIZABLEXACT **xacts;
} PredicateLockData;
```

**Called From**: `src/backend/catalog/system_views.sql` - `pg_locks` view

### PageIsPredicateLocked() - Query Interface

**Signature**:
```c
bool PageIsPredicateLocked(Relation relation, BlockNumber blkno)
```

**Purpose**: Check if page has any predicate locks (for external tools).

### Debug Output

```c
#ifdef PREDICATE_LOCK_DEBUG
    ereport(DEBUG2,
        (errmsg("Predicate lock acquired: %u/%u/%u/%u for xact %u",
                tag.locktag_field1, tag.locktag_field2,
                tag.locktag_field3, tag.locktag_field4,
                sxact->topXid)));
#endif
```

### Serialization Failure Messages

```c
ereport(ERROR,
    (errcode(ERRCODE_SERIALIZATION_FAILURE),
     errmsg("could not serialize access due to concurrent update"),
     errdetail("pattern: Tin=%u, Tpivot=%u, Tout=%u",
               tin->topXid, tpivot->topXid, tout->topXid),
     errhint("The transaction might succeed if retried.")));
```

---

## Part 6: Performance Tuning

### GUC Parameters

```c
// Max total predicate locks in system
max_predicate_locks = (max_connections * 64)  [default]

// Per-transaction limit
max_predicate_locks_per_transaction = 64  [default]

// Per-relation limit  
max_predicate_locks_per_relation = 
    max_predicate_locks_per_transaction / 10

// SLRU buffer cache for commit history
serializable_buffers = 64  [default, in pages]

// Timeout for lock promotion sweeps
predicate_lock_cleanup_timeout = ?  (internal, not user-configurable)
```

### Memory Overhead Calculation

```
Per-transaction: 200 bytes (SERIALIZABLEXACT)
Per-lock: 64 bytes (PREDICATELOCK + target reference)
Per-conflict: 48 bytes (RWConflictData)
Per-xid-mapping: 32 bytes (SERIALIZABLEXID)

Example: 100 connections, 50 concurrent serializable txns
├── Txn records: 50 * 200 = 10 KB
├── Avg 10 locks/txn: 50 * 10 * 64 = 32 KB
├── Avg 2 conflicts/txn: 50 * 2 * 48 = 4.8 KB
├── XID mappings: 50 * 32 = 1.6 KB
└── Total overhead: ~49 KB for moderate workload
```

### Monitoring Predicate Locks

```sql
-- View current predicate locks
SELECT * FROM pg_locks WHERE locktype = 'predicate';

-- Count locks per table
SELECT relation::regclass, COUNT(*)
FROM pg_locks
WHERE locktype = 'predicate'
GROUP BY relation
ORDER BY count DESC;

-- Monitor for memory pressure
SELECT COUNT(*) as total_locks
FROM pg_locks
WHERE locktype = 'predicate';
```

---

## Part 7: Error Handling and Edge Cases

### Out of RWConflictPool Memory

```c
if (dlist_is_empty(&RWConflictPool->availableList)) {
    ereport(ERROR,
        (errcode(ERRCODE_OUT_OF_MEMORY),
         errmsg("not enough RWConflict entries to record conflict"),
         errhint("Reduce max_connections or concurrent transactions")));
}
```

### Transaction Promotion to R/W

```c
// Read-only transaction starts
// Then writes something (or DDL)

if (read_only_xact_writes):
    WritableSxactCount++
    
    // May trigger dangerous structure re-evaluation
    // RO-safe guarantee lost
```

### MVCC Visibility Edge Cases

```c
// Tuple visible in one snapshot, not in another
// Both transactions could read different versions
// SSI must detect conflicts with both versions

if (tuple->xmin in range):
    CheckForSerializableConflictOut(..., tuple->xmin, ...)
```

---

## Part 8: Interaction with Other Subsystems

### With Query Planner

Predicate locking doesn't directly affect planning, but:

```
- Sequential vs. index scans have different locking granularity
- Table-level locks from seq scans vs. tuple locks from index scans
- Planner doesn't account for SSI overhead in cost calculations
```

### With VACUUM and ANALYZE

```c
// VACUUM reads using special snapshot
// Not MVCC snapshot, so no predicate locking

// ANALYZE reads using special snapshot  
// Also skips predicate locking

// This is correct: VACUUM/ANALYZE operations
// Don't participate in serializable conflict detection
```

### With Foreign Data Wrappers

```c
// FDW reads using current transaction's snapshot
// But predicate locks only on local tables

// FDW writes may create conflicts with local readers
// But currently not tracked (limitation of FDW integration)
```

### With Window Functions

```c
// Window functions scan frames of rows
// Each visible row checked for conflicts

for each row in window frame:
    if (row visible in snapshot):
        CheckForSerializableConflictOut(...)
```

