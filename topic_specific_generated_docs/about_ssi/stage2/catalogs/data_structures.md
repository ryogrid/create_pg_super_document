# SSI Data Structures Reference Catalog

## Overview

This catalog provides detailed documentation of all key data structures used in the PostgreSQL SSI implementation. Each structure is documented with field descriptions, invariants, lifetime, and synchronization requirements.

---

## SERIALIZABLEXACT - Core Transaction State

**Location**: `./src/include/storage/predicate_internals.h:148`  
**Size**: ~200 bytes  
**Shared Memory**: Yes  
**Pool**: Static array, allocated at startup

### Purpose
Represents a single serializable transaction during its lifetime and beyond (until all overlapping transactions complete).

### Fields

| Field | Type | Size | Purpose |
|-------|------|------|---------|
| `vxid` | VirtualTransactionId | 8 | Process identifier, backend-local |
| `topXid` | TransactionId | 4 | Top-level XID (for mapping from xid) |
| `finishedBefore` | TransactionId | 4 | XID before which this transaction finished |
| `xmin` | TransactionId | 4 | Snapshot's xmin at acquisition time |
| `prepareSeqNo` | SerCommitSeqNo | 8 | Sequence number when marked PREPARED |
| `commitSeqNo` | SerCommitSeqNo | 8 | Sequence number when COMMITTED |
| `SeqNo` (union) | 8 bytes | -- | Earlier conflict commit OR snapshot xmin |
| `flags` | uint32 | 4 | OR'd flags (COMMITTED, DOOMED, etc.) |
| `pid` | int | 4 | Operating system process ID |
| `pgprocno` | int | 4 | Process array index |
| `predicateLocks` | dlist_head | 16 | List of PREDICATELOCK entries |
| `outConflicts` | dlist_head | 16 | Outgoing rw-conflicts (this writes, others read) |
| `inConflicts` | dlist_head | 16 | Incoming rw-conflicts (others write, this reads) |
| `possibleUnsafeConflicts` | dlist_head | 16 | Potential conflicts (for RO-safe detection) |
| `finishedLink` | dlist_node | 16 | Link in FinishedSerializableTransactions |
| `xactLink` | dlist_node | 16 | Link in PredXact active/available list |
| `perXactPredicateListLock` | LWLock | 12 | Per-transaction lock (parallel only) |

### Flags

```c
#define SXACT_FLAG_COMMITTED         0x00000001  // Transaction committed
#define SXACT_FLAG_PREPARED          0x00000002  // Prepared for commit
#define SXACT_FLAG_ROLLED_BACK       0x00000004  // Rolled back
#define SXACT_FLAG_DOOMED            0x00000008  // Will be rolled back
#define SXACT_FLAG_CONFLICT_OUT      0x00000010  // Has conflict out to committed xact
#define SXACT_FLAG_READ_ONLY         0x00000020  // Read-only transaction
#define SXACT_FLAG_DEFERRABLE_WAITING 0x00000040 // Waiting for safe snapshot
#define SXACT_FLAG_RO_SAFE           0x00000080  // Read-only safe
#define SXACT_FLAG_RO_UNSAFE         0x00000100  // Read-only unsafe
#define SXACT_FLAG_SUMMARY_CONFLICT_IN  0x00000200
#define SXACT_FLAG_SUMMARY_CONFLICT_OUT 0x00000400
#define SXACT_FLAG_PARTIALLY_RELEASED   0x00000800
```

### Synchronization

**Protected By**:
- `SerializableXactHashLock` (exclusive for allocation/deallocation)
- `perXactPredicateListLock` (per-transaction, shared for lock list access)
- Partition locks (for individual lock operations)

**Access Patterns**:
- Backend's own `MySerializableXact`: No lock (thread-local)
- Conflict operations: Partition locks + SerializableXactHashLock
- Cleanup operations: SerializableFinishedListLock

### Lifetime

```
1. Created:      GetSerializableTransactionSnapshot()
                 ├─ Allocated from pool
                 ├─ vxid assigned
                 └─ Linked into activeList
                 
2. Active:       During transaction execution
                 ├─ Predicate locks acquired
                 ├─ Conflicts tracked
                 └─ May become DOOMED
                 
3. Prepared:     PreCommit_CheckForSerializationFailure()
                 ├─ prepareSeqNo set
                 ├─ Final dangerous structure check
                 └─ Either COMMITTED or DOOMED
                 
4. Finished:     RecordTransactionCommit() or AbortTransaction()
                 ├─ commitSeqNo set (if committed)
                 ├─ Moved to FinishedSerializableTransactions
                 └─ Locks kept for overlap tracking
                 
5. Released:     ClearOldPredicateLocks()
                 ├─ Predicate locks removed
                 ├─ Conflicts cleared
                 └─ Returned to pool
```

### Example Usage

```c
// Allocate transaction
SERIALIZABLEXACT *sxact = CreatePredXact();

// Record transaction ID
sxact->topXid = GetTopTransactionId();

// Acquire predicate lock
PredicateLockAcquire(&tag);
// dlist_push_tail(&sxact->predicateLocks, &lock->xactLink);

// Detect conflict
if (!RWConflictExists(sxact, other_sxact)) {
    SetRWConflict(sxact, other_sxact);
    // dlist_push_tail(&sxact->outConflicts, &conflict->outLink);
}

// Commit validation
if (SxactIsDoomed(sxact))
    ereport(ERROR, ERRCODE_SERIALIZATION_FAILURE);

// Cleanup
ReleaseOneSerializableXact(sxact, false, true);
```

---

## PREDICATELOCK - Individual Lock Record

**Location**: `./src/include/storage/predicate_internals.h:285`  
**Size**: ~64 bytes  
**Hash Table**: PredicateLockHash  
**Key**: PREDICATELOCKTAG (target + transaction pair)

### Purpose
Represents one transaction's predicate lock on one database object.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `tag` | PREDICATELOCKTAG | Hash key: target + xact pair |
| `targetLink` | dlist_node | Link in target's lock list |
| `xactLink` | dlist_node | Link in transaction's lock list |

### PREDICATELOCKTAG Structure

```c
typedef struct PREDICATELOCKTAG {
    PREDICATELOCKTARGET *myTarget;  // Pointer to target object
    SERIALIZABLEXACT *myXact;       // Transaction holding lock
} PREDICATELOCKTAG;
```

### Synchronization

**Protected By**: Partition lock of target's hash bucket

**Invariants**:
- At most one PREDICATELOCK per (target, transaction) pair
- PREDICATELOCK exists only if target exists
- Removing last lock on target removes target

---

## PREDICATELOCKTARGET - Lock Target Object

**Location**: `./src/include/storage/predicate_internals.h:255`  
**Size**: ~40 bytes + list overhead  
**Hash Table**: PredicateLockTargetHash

### Purpose
Represents a database object that has predicate locks.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `tag` | PREDICATELOCKTARGETTAG | Hash key: unique object identifier |
| `predicateLocks` | dlist_head | List of PREDICATELOCK objects on this target |

### PREDICATELOCKTARGETTAG Encoding

```c
typedef struct PREDICATELOCKTARGETTAG {
    uint32 locktag_field1;  // Database OID
    uint32 locktag_field2;  // Relation/Index OID
    uint32 locktag_field3;  // Block number or InvalidBlockNumber
    uint32 locktag_field4;  // Offset number or type flags
} PREDICATELOCKTARGETTAG;
```

**Encoding by granularity**:

| Granularity | field1 | field2 | field3 | field4 |
|-------------|--------|--------|--------|--------|
| Relation | DatabaseOID | RelationOID | 0 | 0 |
| Page | DatabaseOID | RelationOID | BlockNumber | 0 |
| Tuple | DatabaseOID | RelationOID | BlockNumber | OffsetNumber |

### Synchronization

**Protected By**: Partition lock based on hash(target)

**Lifecycle**:
1. Created when first lock acquired on target
2. Exists as long as any lock on it
3. Deleted when last lock removed

---

## RWConflictData - Rw-Conflict Edge

**Location**: `./src/include/storage/predicate_internals.h:191`  
**Size**: ~40 bytes  
**Pool**: RWConflictPool (pre-allocated)

### Purpose
Represents a read-write conflict or possible unsafe conflict between two transactions.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `outLink` | dlist_node | Link in sxactOut's outConflicts |
| `inLink` | dlist_node | Link in sxactIn's inConflicts |
| `sxactOut` | SERIALIZABLEXACT* | Writer (conflict out) |
| `sxactIn` | SERIALIZABLEXACT* | Reader (conflict in) |

### Semantics

**Real rw-conflict**: sxactOut writes, sxactIn reads
```
sxactOut: WRITE (before sxactIn sees it)
sxactIn:  READ (sees the write or depends on earlier transaction that did)
```

**Possible unsafe conflict**: For read-only optimization
```
activeXact: Read/write transaction  
roXact:     Read-only transaction
```

### Synchronization

**Protected By**: SerializableXactHashLock  
**Allocated from**: RWConflictPool (pre-allocated, no dynamic allocation)

### Lifecycle

1. Created: `SetRWConflict()` or `SetPossibleUnsafeConflict()`
2. Active: Checked during dangerous structure detection
3. Released: `ReleaseRWConflict()` or `ReleaseOneSerializableXact()`
4. Returned to pool: Available for reuse

---

## PredXactListData - Transaction Pool Manager

**Location**: `./src/include/storage/predicate_internals.h:108`  
**Size**: ~200 bytes (excluding pool)  
**Shared Memory**: Yes, singleton

### Purpose
Central manager for SERIALIZABLEXACT pool and global transaction state.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `availableList` | dlist_head | Free SERIALIZABLEXACT entries |
| `activeList` | dlist_head | In-use transaction records |
| `SxactGlobalXmin` | TransactionId | Minimum xmin of active transactions |
| `SxactGlobalXminCount` | int | Reference count for this xmin |
| `WritableSxactCount` | int | Non-read-only transactions |
| `LastSxactCommitSeqNo` | SerCommitSeqNo | Monotonically increasing counter |
| `CanPartialClearThrough` | SerCommitSeqNo | Cleanup marker |
| `HavePartialClearedThrough` | SerCommitSeqNo | Partial cleanup completed through |
| `OldCommittedSxact` | SERIALIZABLEXACT* | Dummy for summarized transactions |
| `element` | SERIALIZABLEXACT* | Pointer to pool array |

### Synchronization

**Protected By**: `SerializableXactHashLock`

**Access**: All backends may read, exclusive lock required for write

### Global State Invariants

```
1. SxactGlobalXmin ≤ all sxact->xmin for active transactions
2. WritableSxactCount ≤ count of active transactions
3. LastSxactCommitSeqNo strictly monotonically increasing
4. CanPartialClearThrough ≤ HavePartialClearedThrough
```

---

## SERIALIZABLEXID - XID to Transaction Mapping

**Location**: `./src/include/storage/predicate_internals.h:221`  
**Size**: ~32 bytes  
**Hash Table**: SerializableXidHash

### Purpose
Maps a TransactionId to its SERIALIZABLEXACT record, even after transaction process terminates.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `tag` | SERIALIZABLEXIDTAG | Hash key: {xid} |
| `myXact` | SERIALIZABLEXACT* | Pointer to transaction record |

### Why Needed

```
1. Predicate locks reference SERIALIZABLEXACT*
2. Lock target references SERIALIZABLEXACT*
3. Conflict detection references SERIALIZABLEXACT*
4. But backend process may terminate (connection closed)
5. Xid survives termination, pointers don't
6. SERIALIZABLEXID provides mapping xid → pointer
```

### Synchronization

**Protected By**: `SerializableXactHashLock`

---

## LOCALPREDICATELOCK - Backend-Local Cache

**Location**: `./src/backend/storage/lmgr/predicate.c`  
**Size**: ~32 bytes per entry  
**Storage**: Backend-local hash table
**Lifetime**: Per transaction

### Purpose
Cache of predicate locks held by current transaction, used for coalescing decisions.

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `tag` | PREDICATELOCKTARGETTAG | Hash key: lock target |
| `held` | bool | Whether lock physically exists in shared table |
| `childLocks` | int | Count of child locks (simpler tracking) |

### Usage Example

```c
// When considering acquiring tuple lock
if (LocalPredicateLockHash already has page lock on same page) {
    // Page lock covers all tuples on page
    // Don't acquire finer-grained tuple lock
    return;  // Already covered
}

// If too many locks on same page
if (childLocks > threshold) {
    // Coalesce tuple locks → page lock
    // Update LocalPredicateLockHash
}
```

---

## Key Data Structure Relationships

```
SERIALIZABLEXACT (transaction record)
├─ predicateLocks → [PREDICATELOCK entries]
│   └─ Each PREDICATELOCK.tag.myTarget → PREDICATELOCKTARGET
│       └─ target.predicateLocks → [All locks on this target]
│
├─ outConflicts → [RWConflictData entries]
│   └─ Each conflict.sxactIn → Another SERIALIZABLEXACT
│       └─ Other's inConflicts refers back
│
└─ inConflicts → [RWConflictData entries]
    └─ Each conflict.sxactOut → Another SERIALIZABLEXACT


Hash Tables:
├─ PredicateLockTargetHash[tag] → PREDICATELOCKTARGET
├─ PredicateLockHash[tag] → PREDICATELOCK
├─ SerializableXidHash[xid] → SERIALIZABLEXID → SERIALIZABLEXACT
└─ LocalPredicateLockHash[tag] → LOCALPREDICATELOCK
```

---

## Memory Layout Example

```
For transaction T1 holding locks on relation R and page P:

SERIALIZABLEXACT (T1)
├─ vxid: (1, 5)
├─ topXid: 1000
├─ xmin: 990
├─ predicateLocks: dlist
│   ├─ → PREDICATELOCK (rel-lock)
│   │   └─ tag: {R, T1}
│   │   └─ targetLink → PREDICATELOCKTARGET(R)
│   │
│   └─ → PREDICATELOCK (page-lock)
│       └─ tag: {P, T1}
│       └─ targetLink → PREDICATELOCKTARGET(P)
│
├─ outConflicts: dlist
│   └─ → RWConflictData
│       ├─ sxactOut: T1
│       ├─ sxactIn: T2
│       └─ inLink → T2.inConflicts
│
└─ inConflicts: dlist
    └─ empty (T1 is writer, not reader)
```

