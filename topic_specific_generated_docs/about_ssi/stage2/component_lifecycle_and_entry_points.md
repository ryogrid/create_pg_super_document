# PostgreSQL SSI: Transaction Lifecycle and Entry Points

## Overview

The SSI (Serializable Snapshot Isolation) transaction lifecycle consists of key entry points that coordinate between the transaction manager (`xact.c`), snapshot manager (`snapmgr.c`), and predicate locking subsystem (`predicate.c`). This component describes how SSI transactions are born, monitored, and terminated within PostgreSQL's transactional architecture.

**Key Insight**: SSI transactions require special handling at three critical moments:
1. **Snapshot acquisition** - when transaction isolation is set and snapshot is taken
2. **Concurrent operation** - as data is accessed (reads and writes)
3. **Commit validation** - before committing to check for serialization failures

## Architecture

```
Transaction Lifecycle Arc
├── BEGIN
│   └── SetTransactionIsolationLevel()
│       └── Set isolation = SERIALIZABLE
├── Snapshot Acquisition
│   └── GetTransactionSnapshot()
│       └── GetSerializableTransactionSnapshot()
│           └── RegisterPredicateLockingXid()
├── Active Operation Phase
│   ├── Read operations
│   │   └── CheckForSerializableConflictOut()
│   └── Write operations
│       └── CheckForSerializableConflictIn()
├── Commit Phase
│   └── CommitTransaction()
│       ├── PreCommit_CheckForSerializationFailure()
│       ├── ReleasePredicateLocks(true, false)
│       └── Handle SERIALIZATION_FAILURE
└── Abort Phase
    └── AbortTransaction()
        └── ReleasePredicateLocks(false, false)
```

## Core Functions

### 1. GetSerializableTransactionSnapshot() - Entry Point

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.95 (critical path, entry point)  
**Called From**: `snapmgr.c:GetTransactionSnapshot()` for SERIALIZABLE transactions

#### Signature
```c
Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot)
```

#### Purpose
Primary entry point that acquires or validates a snapshot for a serializable transaction. This is called early in transaction startup when isolation level is SERIALIZABLE or when a snapshot is explicitly requested for a serializable transaction.

#### Detailed Logic Flow

1. **Quick Path Check**: If not a serializable transaction, return snapshot immediately
2. **Call Internal Function**: Delegates to `GetSerializableTransactionSnapshotInt()` with source transaction ID from current backend
3. **Shared State Update**: Updates global transaction state if this is the first serializable transaction to acquire a snapshot
4. **Return**: Returns the established snapshot for use in transaction

#### Integration Points
- **Called by**: `snapmgr.c:GetTransactionSnapshot()` when `IsolationLevel == SERIALIZABLE`
- **Protected by**: `SerializableXactHashLock` for concurrent access to shared state
- **Modifies**: `MySerializableXact` (thread-local pointer to transaction record)

#### Concurrency Invariants
- Must be called once per transaction (typically during BEGIN or first access)
- Safe to call from parallel query workers
- Acquires locks in order: `SerializableXactHashLock` → partition locks as needed

#### Error Conditions
- `ERROR` if out of predicate lock memory (max_predicate_locks_per_transaction exceeded)
- Transaction marked as DOOMED and rollback forced

---

### 2. GetSerializableTransactionSnapshotInt() - Internal Coordinator

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.92 (critical implementation detail)

#### Signature
```c
static Snapshot GetSerializableTransactionSnapshotInt(
    Snapshot snapshot,
    VirtualTransactionId *sourcevxid,
    int sourcepid)
```

#### Purpose
Internal function that handles the core logic of establishing or sourcing a serializable snapshot. Supports both new snapshot creation and copying snapshots from other serializable transactions (for parallel query workers).

#### Implementation Flow (Pseudocode)
```
function GetSerializableTransactionSnapshotInt(snapshot, sourcevxid, sourcepid):
    
    Lock(SerializableXactHashLock)
    
    if (MySerializableXact != InvalidSerializableXact):
        # Already have a transaction record
        if sourcevxid provided:
            # Parallel worker - attach to parent's xact
            sourceXact = lookup sourceXact from sourcevxid
            MySerializableXact = sourceXact
        
        Unlock(SerializableXactHashLock)
        return snapshot
    
    # Need to create new transaction record
    CreatePredXact()  # Allocates SERIALIZABLEXACT from pool
    RegisterPredicateLockingXid()  # Register top-level XID
    
    # Update global transaction minimum XID
    SetNewSxactGlobalXmin()
    
    Unlock(SerializableXactHashLock)
    return snapshot
```

#### Key Decision Points

**When to attach to parent transaction** (parallel query):
- `sourcevxid` is not NULL (indicates parallel worker)
- Parent transaction exists and is serializable
- Current backend's transaction level >= parent's

**When to read-only optimize**:
- Not implemented immediately; determined at commit time
- Checked when `SxactIsROSafe()` becomes true

#### Field Modifications
- `MySerializableXact` - Set to point to transaction record
- `snapshot->xmin` - May be adjusted based on global state
- `TransactionState->parallelMaster` - Updated if parallel worker

---

### 3. RegisterPredicateLockingXid() - XID Registration

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.85 (critical for transaction identification)

#### Signature
```c
void RegisterPredicateLockingXid(void)
```

#### Purpose
Creates mapping from the transaction's top-level `TransactionId` to its `SERIALIZABLEXACT` record. This mapping is essential because predicate locks must survive transaction process termination but still be associated with the original transaction.

#### Implementation Details

**When called**: After `SERIALIZABLEXACT` is created but before any data access  
**Precondition**: `MySerializableXact` must be valid  
**Lock scope**: Held under `SerializableXactHashLock` (exclusive)

#### Hash Table Mapping
```c
SERIALIZABLEXID {
    tag: { xid: <top-level-xid> }
    myXact: <pointer-to-SERIALIZABLEXACT>
}
```

**Why this is needed**:
1. Predicate locks are stored by `PREDICATELOCKTARGETTAG` + `SERIALIZABLEXACT*` pointer
2. Transaction process can terminate before all concurrent transactions finish
3. Conflict detection must still find the transaction's predicate locks
4. XID-to-SXACT mapping allows lookup by XID after process termination

#### Special Case: Subtransactions
- Uses `SubTransGetTopmostTransaction()` to get top-level XID
- All subtransactions map to same parent's `SERIALIZABLEXACT`
- Subtransaction rollback doesn't invalidate mapping

---

### 4. CreatePredXact() - Allocation

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.80 (critical for shared memory management)

#### Signature
```c
static SERIALIZABLEXACT *CreatePredXact(void)
```

#### Purpose
Allocates a `SERIALIZABLEXACT` record from the shared memory pool. This is the entry point for transaction tracking in SSI.

#### Logic
```c
// Get free entry from pool (or return NULL if exhausted)
sxact = dlist_pop_head_node(&PredXact->availableList)

// Move from available list to active list
dlist_push_tail(&PredXact->activeList, &sxact->xactLink)

// Initialize fields
memset(sxact->flags, 0)
sxact->vxid = GetMyVirtualTransactionId()
sxact->pid = MyProcPid
sxact->xmin = GetTransactionSnapshot()->xmin
sxact->topXid = InvalidTransactionId  // Set later by RegisterPredicateLockingXid

return sxact
```

#### Resource Constraints
- Pool size = `max_connections * (1 + max_prepared_xacts)`
- Allocation failure → `ERROR` with out-of-memory message
- Recommends reducing concurrent transactions or increasing `max_connections`

#### Fields Initialized
| Field | Value | Meaning |
|-------|-------|---------|
| `vxid` | Current process VirtualTransactionId | Process identifier |
| `prepareSeqNo` | 0 (invalid) | Not yet prepared for commit |
| `commitSeqNo` | 0 (invalid) | Not yet committed |
| `flags` | 0 | Initial state (none of COMMITTED, PREPARED, etc.) |
| `predicateLocks` | empty list | No locks yet acquired |
| `outConflicts` | empty list | No write conflicts yet |
| `inConflicts` | empty list | No read conflicts yet |

---

### 5. ReleasePredicateLocks() - Cleanup

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.90 (critical path for cleanup)

#### Signature
```c
void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe)
```

#### Parameters
- `isCommit` (bool): Whether transaction is committing (true) or aborting (false)
- `isReadOnlySafe` (bool): Whether read-only transaction determined to be safe

#### Purpose
Releases all predicate locks for the transaction and updates global transaction state. Called at transaction end (commit/abort) and also when read-only transaction becomes "safe".

#### Logic Flow

**If `isReadOnlySafe == true`:**
```
// Early release for RO-safe transaction
Mark transaction as RO_SAFE
Release all predicate locks immediately
Return to transaction
// Transaction continues but no longer participates in SSI
```

**If `isCommit == true` (normal commit):**
```
Mark transaction as COMMITTED
Record commitSeqNo = ++LastSxactCommitSeqNo
Move from active list to finished list
Keep locks alive until all concurrent transactions complete
// Locks released by ReleaseOneSerializableXact() later
```

**If `isCommit == false` (abort):**
```
Mark transaction as ROLLED_BACK  
Remove from all conflict lists
Release predicate locks immediately
// Transaction no longer participates in conflicts
```

#### RO-Safe Optimization Details

**What makes a transaction "RO-safe"?**
- Read-only transaction with no incoming conflicts from other serializable transactions
- All concurrent write transactions have committed without conflicts out
- Proof: If no W→R conflict, no cycle possible, snapshot is serializable

**How it's determined**:
- Checked at commit time in `PreCommit_CheckForSerializationFailure()`
- For DEFERRABLE transactions, checked during `GetSafeSnapshot()` wait loop
- Once marked RO_SAFE, locks released immediately - no need for cleanup

#### Concurrency Considerations
- Must hold `SerializablePredicateListLock` to walk lock list
- May hold multiple partition locks during cleanup
- Lock release order critical to avoid deadlock

---

### 6. ReleasePredXact() - Deallocation

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.78 (support function)

#### Signature
```c
static void ReleasePredXact(SERIALIZABLEXACT *sxact)
```

#### Purpose
Returns a `SERIALIZABLEXACT` record to the available pool after it's no longer needed.

#### When Called
- After all concurrent transactions have completed
- Called from `ReleaseOneSerializableXact()` during cleanup sweep
- Triggered by `ClearOldPredicateLocks()` when `SxactGlobalXmin` advances

#### Operation
```c
// Remove from active list
dlist_delete(&sxact->xactLink)

// Return to available pool
dlist_push_tail(&PredXact->availableList, &sxact->xactLink)

// Invariants checked
Assert(sxact is not the current backend's transaction)
Assert(no predicate locks remain attached)
Assert(no conflicts remain in lists)
```

---

### 7. SetSerializableTransactionSnapshot() - Snapshot Duplication

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.82 (parallel query support)

#### Signature
```c
void SetSerializableTransactionSnapshot(
    Snapshot snapshot,
    VirtualTransactionId *sourcevxid,
    int sourcepid)
```

#### Purpose
Allows parallel query worker to use the same `SERIALIZABLEXACT` record as its parent transaction. Called during parallel worker initialization.

#### Mechanism
- Worker backend calls with parent's `VirtualTransactionId` and PID
- Looks up parent's transaction record in shared state
- Attaches worker to parent's `SERIALIZABLEXACT` (worker doesn't get its own)
- Multiple workers share single transaction record for conflict tracking

#### Invariants
- Parent's `SERIALIZABLEXACT` remains owner of all predicate locks
- Worker's data access conflicts still attributed to parent transaction
- Worker termination doesn't invalidate locks or conflicts
- Parent must be alive when worker accesses data

---

### 8. SetNewSxactGlobalXmin() - Global Transaction Minimum

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.75 (critical for memory management)

#### Signature
```c
static void SetNewSxactGlobalXmin(void)
```

#### Purpose
Updates the global minimum `xmin` (snapshot timestamp) across all active serializable transactions. This determines what transaction records can be cleaned up.

#### Logic
```
function SetNewSxactGlobalXmin():
    Lock(SerializableXactHashLock)
    
    newMin = InvalidTransactionId
    count = 0
    
    for each transaction in PredXact->activeList:
        if not already rolled back:
            if newMin is invalid OR xmin is earlier:
                newMin = sxact->xmin
            count++
    
    // Update global state
    if newMin changed:
        PredXact->SxactGlobalXmin = newMin
        PredXact->SxactGlobalXminCount = count
        
        // Trigger cleanup sweep if not recently done
        if time since last ClearOldPredicateLocks > threshold:
            ScheduleCleanup()
    
    Unlock(SerializableXactHashLock)
```

#### Cleanup Implications
- Only transactions that **began after** global xmin can be cleaned
- Commit information retained in SLRU until transaction older than global xmin
- Memory pressure handled by promoting fine-grained locks to coarser granularity

---

## Transaction State Transitions

```
CREATE  ─→  ACTIVE  ─→  PREPARED  ─→  COMMITTED/ROLLED_BACK
          ├─ acquire locks
          ├─ detect conflicts
          ├─ check for cycles
          └─ can become DOOMED

Doomed Transactions:
DOOMED ─→ forced abort during conflict detection
       ─→ cleanup same as normal rollback
```

## Global State Management

### PredXactListData Structure
Located in shared memory, manages pool of transaction records.

```c
typedef struct PredXactListData {
    dlist_head  availableList;        // Free SERIALIZABLEXACT records
    dlist_head  activeList;           // In-use transaction records
    
    TransactionId SxactGlobalXmin;   // Oldest active xmin
    int           SxactGlobalXminCount;
    int           WritableSxactCount;  // Non-read-only txns
    SerCommitSeqNo LastSxactCommitSeqNo;  // Next commit number
    
    SERIALIZABLEXACT *OldCommittedSxact;  // Dummy for summarized txns
} PredXactListData;
```

**Key Invariants**:
- `SxactGlobalXmin` ≥ all snapshots of committed transactions
- `WritableSxactCount` updated when R/O transaction promoted to R/W
- `LastSxactCommitSeqNo` strictly monotonically increasing

---

## Integration with Transaction Manager

### In `xact.c` Integration Points

**At transaction start**:
```c
CommitTransaction() {
    if (IsolationLevel == SERIALIZABLE) {
        PreCommit_CheckForSerializationFailure();  // Validate
        ReleasePredicateLocks(true, false);        // Cleanup
    }
}
```

**At transaction abort**:
```c
AbortTransaction() {
    ReleasePredicateLocks(false, false);  // Immediate cleanup for aborts
}
```

**For subtransactions**:
```c
PredicateLockAcquire() {
    // All predicate locks use top-level xid mapping
    // Subtransaction abort doesn't affect predicate locks
    // Only top-level abort or commit triggers cleanup
}
```

---

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| GetSerializableTransactionSnapshot | O(1) | Single hash table lookup + SERIALIZABLEXACT creation |
| RegisterPredicateLockingXid | O(1) | Single hash table insertion |
| SetNewSxactGlobalXmin | O(n) | n = number of active serializable txns |
| ReleasePredicateLocks | O(m) | m = number of predicate locks held |

### Memory Overhead

- Per transaction: ~200 bytes (SERIALIZABLEXACT struct)
- Per lock: ~64 bytes (PREDICATELOCK + PREDICATELOCKTARGET reference)
- Global pool: `max_connections * (1 + max_prepared_xacts)` transaction slots

---

## Special Cases and Edge Cases

### Parallel Query Workers
- Worker inherits parent's `SERIALIZABLEXACT` pointer
- All locks acquired by worker attributed to parent transaction
- Parent's cleanup deferred until all workers terminate

### Prepared Transactions
- `SERIALIZABLEXACT` record preserved across PREPARE/COMMIT (2PC)
- Predicate locks written to WAL for recovery
- Conflict information retained in SLRU

### Read-Only Transactions
- May be released early if determined "safe"
- "Safe" = snapshot doesn't have conflicts with write transactions
- Optimization reduces memory pressure and cleanup overhead

### Subtra transactions
- Use parent's `SERIALIZABLEXACT` record
- Rollback of subtransaction doesn't affect predicate locks
- All predicate locks released only when top-level transaction ends

---

## Debugging and Observability

### Key Debug Macros
```c
#ifdef PREDICATE_LOCK_DEBUG
    // Detailed logging of lock acquisition/release
    ereport(DEBUG2, (errmsg(...)));
#endif

// Trace points for serious problems
ereport(ERROR, (errcode(ERRCODE_...)))
```

### Monitoring Queries
```sql
-- View active serializable transactions
SELECT * FROM pg_stat_activity 
WHERE iso_level = 'serializable';

-- Predicate locks via pg_locks view
SELECT * FROM pg_locks 
WHERE locktype = 'predicate';
```

### GUC Parameters
- `max_predicate_locks_per_transaction` - Max locks per transaction
- `max_predicate_locks_per_relation` - Max locks per table
- `serializable_buffers` - SLRU buffer pool size for commit history

