# PostgreSQL SSI: Snapshot and Registration

## Overview

The snapshot management subsystem in SSI ensures that each serializable transaction operates on a consistent point-in-time view of the database while also collecting metadata necessary for conflict detection. This component bridges MVCC visibility logic with SSI's transaction ordering requirements.

**Key Insight**: SSI relies on snapshots to determine transaction visibility AND to track relative transaction ordering. The same snapshot xmin/xmax/xip arrays used for MVCC visibility are also used to identify potential read-write conflicts.

## Architecture

```
Snapshot Lifecycle for SSI
├── GetTransactionSnapshot()
│   └── [if SERIALIZABLE]
│       └── GetSerializableTransactionSnapshot()
│           ├── Allocate SERIALIZABLEXACT
│           ├── Initialize snapshot fields
│           ├── Update GlobalXmin
│           └── Return snapshot
├── Snapshot Validation
│   ├── IsSnapshotCurrent() checks
│   └── XIP array membership tests
└── Snapshot Usage
    ├── Tuple visibility checks
    │   └── HeapTupleSatisfiesMVCC()
    ├── Conflict detection
    │   └── CheckForSerializableConflictOut()
    └── Index scans
        └── Check predicate lock targets
```

## Core Concepts

### Snapshot vs. SERIALIZABLEXACT Relationship

Both structures track transaction state but serve different purposes:

| Aspect | Snapshot | SERIALIZABLEXACT |
|--------|----------|------------------|
| **Lifetime** | Per-query (or per-command) | Per-transaction |
| **Visibility** | Determines which tuple versions visible | Determines conflict history |
| **Fields** | xmin, xmax, xip[] | vxid, topXid, commitSeqNo |
| **Usage** | MVCC logic | Conflict graph detection |
| **Scope** | Local to backend | Shared memory |

### Key Snapshot Fields Used in SSI

```c
typedef struct SnapshotData {
    // MVCC visibility fields
    TransactionId xmin;       // Oldest committed xid visible
    TransactionId xmax;       // Truncation horizon (next to assign)
    TransactionId *xip;       // Array of in-progress xids
    uint32 xcnt;             // Count of xip entries
    
    // SSI-specific metadata
    Snapshot registered_xmin; // For safe snapshot detection
    int16 suboverflowed;      // Subtransaction list overflow indicator
} SnapshotData;
```

## Snapshot Acquisition

### GetSafeSnapshot() - Read-Only Optimization

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.90 (read-only optimization)

#### Signature
```c
static Snapshot GetSafeSnapshot(Snapshot origSnapshot)
```

#### Purpose
For DEFERRABLE read-only transactions, waits for a "safe" snapshot where the transaction is guaranteed not to have serialization conflicts. This optimization allows read-only transactions to execute with minimal locking overhead.

#### Algorithm

```
function GetSafeSnapshot(origSnapshot):
    
    // Invariant: this is a read-only DEFERRABLE transaction
    Assert(MySerializableXact.flags & SXACT_FLAG_READ_ONLY)
    Assert(IsolationLevel == SERIALIZABLE)
    
    Loop:
        // Check if current snapshot is safe
        canCommit = CheckForSafeSnapshot()
        
        if canCommit:
            // All concurrent R/W txns have committed
            // No conflicts possible
            Mark MySerializableXact as RO_SAFE
            Return origSnapshot
        
        // Not safe yet - wait for concurrent transactions
        LWLockAcquire(SerializableFinishedListLock, LW_SHARED)
        
        // Check again under lock (condition variable)
        canCommit = CheckForSafeSnapshot()
        
        if canCommit:
            LWLockRelease(...)
            Mark MySerializableXact as RO_SAFE
            Return origSnapshot
        
        // Sleep - other transactions will signal when they commit
        cv_wait(&SerializableFinishedListCondvar, lock)
        
        LWLockRelease(...)
        // Loop back to check again
```

#### Safety Condition

A snapshot is "safe" if **no read-write transaction** could create a cycle:

**Mathematical proof sketch**:
- Let T_RO = current read-only transaction
- For a cycle: there must exist T_1 → T_2 → ... → T_n → T_RO → T_1
- T_RO is read-only, so no T_i can write after T_RO reads
- Therefore no edge T_RO → T_x is possible
- No cycle can exist ✓

**Practical check**:
- All write transactions that overlap with T_RO snapshot must have committed
- No T_RO.read conflicts with in-progress write transactions
- Check: `possibleUnsafeConflicts` list is empty after all concurrent R/W commits

#### When a Transaction Becomes Safe

1. **After second barrier point**: All transactions concurrent with T_RO have committed
2. **Checked at cleanup**: ClearOldPredicateLocks() verifies safety
3. **Marked in flags**: SXACT_FLAG_RO_SAFE set once confirmed

#### DEFERRABLE Semantics
- SQL: `SET TRANSACTION DEFERRABLE`
- Backend effect: Blocks at `GetSafeSnapshot()` until safe snapshot available
- Timeout: None specified in standard SQL; PostgreSQL waits indefinitely
- Usefulness: Heavy batch reads benefit from guaranteed no-retry semantics

---

### Snapshot Registration and Xmin Tracking

**Source**: `./src/backend/storage/lmgr/predicate.c`

#### xmin Snapshot Xmin Field

During `GetSerializableTransactionSnapshot()`, the backend-local snapshot's xmin is recorded:

```c
snapshot->xmin = GetTransactionSnapshot()->xmin

// Later recorded in transaction structure
MySerializableXact->xmin = snapshot->xmin
```

**Why this matters**:
- All tuples modified by transactions with xid ≥ `snapshot->xmin` are definitely visible to this transaction
- Tuples modified by transactions with xid < `snapshot->xmin` are definitely hidden
- Transactions with xid < `snapshot->xmin` are "committed before" this transaction's view

#### GlobalXmin Advancement

After registering new serializable transaction:

```c
SetNewSxactGlobalXmin() {
    // Find minimum xmin among all active serializable transactions
    globalMin = MIN(sxact->xmin for all sxact in activeList)
    
    // This becomes the new SxactGlobalXmin
    PredXact->SxactGlobalXmin = globalMin
    
    // Only transactions that started AFTER this xmin can be cleaned
    // This implements reference-counting via xmin
}
```

**Impact on cleanup**:
- Determines when `SERIALIZABLEXACT` records can be deallocated
- Determines when predicate locks can be released
- Determines when SLRU pages can be recycled

---

## Snapshot Interaction with Visibility

### HeapTupleSatisfiesMVCC() - Visibility Check

**Source**: `./src/backend/access/heap/heapam_visibility.c`  
**Importance**: Not directly part of SSI but critical context

The MVCC visibility check compares tuple xmin/xmax against snapshot:

```c
HeapTupleSatisfiesMVCC(HeapTuple tuple, Snapshot snapshot) {
    
    TransactionId xmin = tuple->xmin;
    TransactionId xmax = tuple->xmax;
    
    // Tuple created by committed transaction?
    if (TransactionIdIsCurrentTransactionId(xmin))
        return true;  // Own insert, definitely visible
    
    // Tuple deleted or updated by concurrent transaction?
    if (!TransactionIdPrecedes(xmin, snapshot->xmin))
        return false;  // Inserted after snapshot, not visible
    
    // Is tuple deleted?
    if (TransactionIdIsValid(xmax)) {
        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;  // Own delete, not visible
        if (!TransactionIdPrecedes(xmax, snapshot->xmin))
            return true;  // Delete after snapshot, tuple still visible
        return false;  // Deleted before snapshot
    }
    
    return true;  // Committed before snapshot, not deleted - visible!
}
```

### Conflict Detection Integration

When reading a tuple with xmin X:

```c
// In CheckForSerializableConflictOut()
if (tuple_xid > snapshot->xmin) {
    // Tuple written by concurrent transaction
    // Check if that transaction conflicts with ours
    CheckForSerializableConflictOut(
        relation, tuple_xid, snapshot)
}
```

---

## Multitransaction (MVCC) Details

### Multitransaction XMAX Handling

PostgreSQL uses Multitransaction (xid_t mxid) to track locks held during visibility checks:

```c
// In predicate.c conflict detection:
if (HeapTupleHeaderGetXmax(tuple->t_data) > FirstMultiXactId) {
    // XMAX is a multitransaction
    // Expand to constituent XIDs
    // Check each for conflicts
}
```

**Why necessary**:
- Multiple transactions can have locks on same tuple
- Need to detect conflicts with all of them
- Multitransaction bitmask indicates which members

---

## Snapshot Implementation in Predicate Locking

### Snapshot Copy for Serializable Transactions

```c
GetSerializableTransactionSnapshot(Snapshot snapshot) {
    // Take reference snapshot
    if (!snapshot)
        snapshot = GetTransactionSnapshot();
    
    // Record snapshot in transaction record
    MySerializableXact->xmin = snapshot->xmin;
    MySerializableXact->lastCommitBeforeSnapshot = 
        GetLastCommitSeqNo();
    
    // Return snapshot for query processing
    return snapshot;
}
```

### Snapshot Staleness Checks

Before using snapshot for query execution:

```c
bool IsSnapshotCurrent(Snapshot snapshot) {
    // Check if snapshot is still valid
    if (GetTransactionIsolationLevel() == SERIALIZABLE) {
        // Serializable snapshots don't become stale
        // Only one snapshot per transaction
        return true;
    }
    
    // For other isolation levels, check horizon movement
    return (snapshot->xmin >= GetMyXmin());
}
```

---

## Snapshot and Subtransactions

### Subtransaction Snapshot Handling

PostgreSQL maintains a stack of snapshots for nested transactions:

```c
typedef struct TransactionStateData {
    Snapshot snapshot;           // Active snapshot
    Snapshot curcid_snapshot;    // For command ID tracking
    struct TransactionStateData *parent;
} TransactionStateData;
```

**In SSI context**:
- Subtransaction uses parent's `SERIALIZABLEXACT` record
- Subtransaction snapshot is SAME as parent snapshot
- All predicate locks acquired by subtransaction charged to parent
- Subtransaction commit/rollback doesn't affect predicate locks

### Subtransaction Abort Semantics

When subtransaction rolls back:

```c
AtAbort_Memory() {
    // Release subtransaction-local memory
    // But DON'T release predicate locks
    // They remain as they're recorded by parent xid
}

ReleasePredXact() {
    // NOT called for subtransaction abort
    // Parent transaction still has write history recorded
}
```

---

## Snapshot Comparison and Ordering

### Snapshot Happened-Before Ordering

SSI uses snapshot ordering to determine transaction precedence:

```c
bool SnapshotPrecedesTransaction(
    Snapshot snapshot,           // Earlier transaction's snapshot
    SERIALIZABLEXACT *laterTxn)  // Later transaction
{
    // If we can prove laterTxn could see snapshot's effects
    if (laterTxn->xmin >= snapshot->xmax) {
        // Every transaction in laterTxn's snapshot started after
        // every transaction in snapshot's view ended
        // No conflicts possible
        return true;
    }
    
    return false;
}
```

### Safe Snapshot Determination

For read-only optimization:

```c
bool CheckForSafeSnapshot() {
    SERIALIZABLEXACT *sxact;
    
    // Current RO transaction is safe if:
    // 1. No concurrent R/W transactions remain
    for each writableSxact in activeList:
        if (writableSxact->xmin < MySerializableXact->xmin) {
            // R/W transaction overlaps our snapshot
            return false;
        }
    
    // 2. All transactions that could conflict are committed
    for each conflict in MySerializableXact->possibleUnsafeConflicts:
        if (!SxactIsCommitted(conflict->sxactOut)) {
            // Potential conflict not yet resolved
            return false;
        }
    
    return true;  // Safe!
}
```

---

## Memory and Performance Implications

### Snapshot Memory Overhead

Per transaction:
- Snapshot structure: ~100 bytes
- XIP array: ~8 bytes × (typical concurrent writers)
- SERIALIZABLEXACT: ~200 bytes

Total: ~300-500 bytes depending on workload

### Snapshot Copy Optimization

PostgreSQL reuses snapshot objects when possible:

```c
// Snapshot lifetime management
typedef enum {
    SNAPSHOT_MVCC,          // Regular snapshot
    SNAPSHOT_SELF,          // Tuple from own transaction
    SNAPSHOT_ANY,           // Include all tuples
    SNAPSHOT_TOAST,         // Include TOAST
    SNAPSHOT_DIRTY,         // Include uncommitted
    SNAPSHOT_NON_VACUUMABLE // For vacuum
} SnapshotType;
```

For serializable transactions, SNAPSHOT_MVCC is required - cannot use special snapshots.

---

## Data Structures

### Snapshot Structure Details

```c
typedef struct SnapshotData {
    SnapshotType snapshot_type;

    // Visibility data
    TransactionId xmin;         // All xids < xmin are visible
    TransactionId xmax;         // All xids >= xmax are not visible
    TransactionId *xip;         // Array of in-progress xids
    uint32 xcnt;               // Count of xip
    
    // Multitransaction support
    MultiXactId mxactoff;       // Multixact array
    MultiXactId *mxact;
    uint32 mxcnt;

    // Subxid tracking
    TransactionId *subxip;
    int32 subxcnt;
    bool suboverflowed;

    // Command ID for command-level visibility
    CommandId curcid;

    // Timestamp for consistency checks
    TimestampTz whenTaken;
    unsigned int copied:1;
} SnapshotData;
```

### SERIALIZABLEXACT Snapshot Fields

```c
typedef struct SERIALIZABLEXACT {
    // ... other fields ...
    
    TransactionId xmin;  // Snapshot's xmin at acquisition
    
    SerCommitSeqNo lastCommitBeforeSnapshot;
    // or
    SerCommitSeqNo earliestOutConflictCommit;
    
    // ... rest of struct ...
} SERIALIZABLEXACT;
```

---

## Integration Points

### With snapmgr.c

The snapshot manager (`snapmgr.c`) handles snapshot lifecycle:

```c
GetTransactionSnapshot() {
    // Called at query/command boundary
    
    if (IsolationLevel == SERIALIZABLE) {
        // Route through SSI
        return GetSerializableTransactionSnapshot(
            GetOldestXmin());
    } else {
        // Regular snapshot path
        return GetSnapshotData(...);
    }
}
```

### With heapam.c and visibility checks

When scanning tuples:

```c
heapgetpage() {
    // For each tuple in page:
    if (!tuple_visible_in_snapshot(tuple, snapshot)) {
        continue;  // Skip invisibletuples
    }
    
    if (SERIALIZABLE_ISOLATION()) {
        CheckForSerializableConflictOut(
            relation,
            HeapTupleHeaderGetXmin(tuple->t_data),
            snapshot);
    }
    
    // Process tuple
}
```

### With parallel query workers

```c
SetSerializableTransactionSnapshot(snapshot, 
                                    sourcevxid,
                                    sourcepid) {
    // Worker reuses parent's snapshot
    // And parent's SERIALIZABLEXACT
    // All conflicts attributed to parent
}
```


---

## Prerequisites
- Complete understanding of all prior chapters (especially Chapter 03)
- Familiarity with PostgreSQL transaction isolation and MVCC
- Understanding of shared memory and LWLock synchronization

## Next Steps
→ [Chapter 5: 05 *](../final/05_*.md)
→ [Back to Architecture Overview](02_architecture_overview.md)
→ [Jump to Deep Dives](18_deep_dives.md) for advanced topics
