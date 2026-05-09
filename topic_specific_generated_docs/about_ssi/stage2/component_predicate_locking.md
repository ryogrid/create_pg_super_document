# PostgreSQL SSI: Predicate Lock Acquisition and Management

## Overview

Predicate locking is the core mechanism of SSI that enables conflict detection. Unlike regular locks (which prevent access), predicate locks are *non-blocking* markers that indicate "this transaction examined these database objects." They serve two purposes:

1. **Recording what was read**: Track tuple/page/relation scans at various granularities
2. **Detecting conflicts**: When another transaction writes, check if it overlaps with existing predicate locks

**Key Insight**: Predicate locks don't prevent operations; they're flags that enable post-hoc conflict analysis.

## Architecture

```
Lock Granularity Hierarchy (finest to coarsest)
├── TUPLE-level locks
│   └── Acquire: PredicateLockTID()
│       └── On: specific (relation, page, tid)
├── PAGE-level locks
│   └── Acquire: PredicateLockPage()
│       └── On: (relation, page)
├── RELATION-level locks
│   └── Acquire: PredicateLockRelation()
│       └── On: (relation)
└── INDEX-RANGE-PROXY locks
    └── Implicit in index scans
        └── Represents: all index entries matching predicate

Lock Coalescing Flow
├── Tuple locks acquired during scan
├── Check: do we have too many fine-grained locks?
│   └── If > max_predicate_locks_per_transaction
├── Promote: merge to page-level locks
│   └── CheckAndPromotePredicateLockRequest()
└── Further promote: relation-level locks
    └── If too many page locks
```

## Core Data Structures

### PREDICATELOCK - Individual Lock Record

**Source**: `./src/include/storage/predicate_internals.h`

```c
typedef struct PREDICATELOCKTAG {
    PREDICATELOCKTARGET *myTarget;  // Pointer to target object
    SERIALIZABLEXACT *myXact;       // Transaction holding lock
} PREDICATELOCKTAG;

typedef struct PREDICATELOCK {
    // Hash key
    PREDICATELOCKTAG tag;
    
    // Links to maintain lists
    dlist_node targetLink;  // Link in target's lock list
    dlist_node xactLink;    // Link in transaction's lock list
} PREDICATELOCK;
```

**Key invariant**: A single `(target, transaction)` pair has at most ONE `PREDICATELOCK` entry.

### PREDICATELOCKTARGET - Lock Target Object

```c
typedef struct PREDICATELOCKTARGETTAG {
    uint32 locktag_field1;  // Database OID
    uint32 locktag_field2;  // Relation/Index OID
    uint32 locktag_field3;  // Block number (or flags)
    uint32 locktag_field4;  // Item pointer or type info
} PREDICATELOCKTARGETTAG;

typedef struct PREDICATELOCKTARGET {
    PREDICATELOCKTARGETTAG tag;
    dlist_head predicateLocks;  // List of PREDICATELOCK objects
} PREDICATELOCKTARGET;
```

**Lock Target Encoding**:

| Granularity | field1 | field2 | field3 | field4 |
|-------------|--------|--------|--------|--------|
| Relation | DbOid | RelOid | InvalidBlockNumber | 0 |
| Page | DbOid | RelOid | BlockNumber | 0 |
| Tuple | DbOid | RelOid | BlockNumber | OffsetNumber |
| IndexRange | DbOid | IndexOid | LowerBound | UpperBound |

---

## Public Lock Acquisition APIs

### 1. PredicateLockRelation() - Relation-Level Lock

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.92 (critical acquisition point)

#### Signature
```c
void PredicateLockRelation(Relation relation, Snapshot snapshot)
```

#### Parameters
- `relation` (Relation*): The heap or index relation object
- `snapshot` (Snapshot): Current transaction snapshot

#### Purpose
Acquires a coarse-grained predicate lock on an entire relation. Used when:
- Sequential scan of entire table
- Bulk operations (INSERT ... SELECT, UPDATE, DELETE)
- Index range scans with unbounded predicates
- Insufficient memory for fine-grained locks

#### Implementation Flow

```c
void PredicateLockRelation(Relation relation, Snapshot snapshot) {
    
    // Quick exit if not serializable or relation doesn't participate
    if (!SerializationNeededForRead(relation, snapshot))
        return;
    
    // Build lock target tag
    PREDICATELOCKTARGETTAG tag = {
        .locktag_field1 = MyDatabaseId,
        .locktag_field2 = relation->rd_id,
        .locktag_field3 = InvalidBlockNumber,
        .locktag_field4 = 0
    };
    
    // Acquire the lock
    PredicateLockAcquire(&tag);
}
```

#### Lock Propagation

After acquiring relation-level lock:
- All page-level locks on the relation become redundant
- All tuple-level locks on the relation become redundant
- Automatic cleanup: DeleteChildTargetLocks(&tag)

#### Callers
- `heapam.c`: Sequential scan without WHERE clause
- `nbtree.c`: Full index scan
- Table reconstruction after REINDEX

---

### 2. PredicateLockPage() - Page-Level Lock

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.90 (common acquisition point)

#### Signature
```c
void PredicateLockPage(
    Relation relation,
    BlockNumber blkno,
    Snapshot snapshot)
```

#### Purpose
Acquires a page-level predicate lock. Used when:
- Single table scan (scans multiple tuples on page)
- Index page access
- CLUSTER operations
- Page-level visibility checks

#### Implementation

```c
void PredicateLockPage(Relation relation, BlockNumber blkno, 
                       Snapshot snapshot) {
    
    if (!SerializationNeededForRead(relation, snapshot))
        return;
    
    // Encode page-level target tag
    PREDICATELOCKTARGETTAG tag = {
        .locktag_field1 = MyDatabaseId,
        .locktag_field2 = relation->rd_id,
        .locktag_field3 = blkno,
        .locktag_field4 = 0  // No item pointer
    };
    
    // Check for promotion opportunity
    if (CoarserLockCovers(&tag)) {
        // Relation-level lock already exists
        // No need for page lock
        return;
    }
    
    // Check if should be promoted to page lock immediately
    if (CheckAndPromotePredicateLockRequest(&tag)) {
        // Promoted due to memory pressure
        return;
    }
    
    PredicateLockAcquire(&tag);
}
```

#### Page Lock Clustering

When multiple page locks exist on same relation:
- Count = number of pages locked
- If count > `max_predicate_locks_per_relation` / 10
- Promote to relation-level lock
- Release all page locks

#### Callers
- Index scan callbacks
- Heap page access routines
- CLUSTER command

---

### 3. PredicateLockTID() - Tuple-Level Lock

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.88 (fine-grained locking)

#### Signature
```c
void PredicateLockTID(
    Relation relation,
    ItemPointer tid,
    Snapshot snapshot,
    TransactionId tuple_xid)
```

#### Parameters
- `relation` (Relation*): The table
- `tid` (ItemPointer): Tuple identifier (page + offset)
- `snapshot` (Snapshot): Transaction snapshot
- `tuple_xid` (TransactionId): XID that wrote the tuple

#### Purpose
Acquires the finest-grained predicate lock. Used when:
- Reading individual tuple
- Heap sequential scan (one lock per visible tuple)
- Index scans with specific tuple retrieval

#### Implementation

```c
void PredicateLockTID(Relation relation, ItemPointer tid,
                      Snapshot snapshot, TransactionId tuple_xid) {
    
    if (!SerializationNeededForRead(relation, snapshot))
        return;
    
    // Check if tuple's xid is "interesting"
    // Don't lock tuples from already-committed transactions in xmin
    if (TransactionIdPrecedes(tuple_xid, snapshot->xmin)) {
        // Tuple was written by transaction before our snapshot
        // Don't need lock for that transaction
        // (would only conflict with WRITE before our READ)
        return;
    }
    
    // Build tuple-level target tag
    PREDICATELOCKTARGETTAG tag = {
        .locktag_field1 = MyDatabaseId,
        .locktag_field2 = relation->rd_id,
        .locktag_field3 = ItemPointerGetBlockNumber(tid),
        .locktag_field4 = ItemPointerGetOffsetNumber(tid)
    };
    
    // Check for coarser locks that cover this tuple
    if (CoarserLockCovers(&tag))
        return;  // Already covered by page or relation lock
    
    // Check if should be promoted
    if (CheckAndPromotePredicateLockRequest(&tag))
        return;  // Promoted due to memory pressure
    
    PredicateLockAcquire(&tag);
}
```

#### Fine-Grained vs. Coarse-Grained Trade-off

**Fine-grained benefits**:
- More specific conflict detection
- Fewer false positives in dangerous structure detection
- Better concurrency (other transactions less likely to conflict)

**Coarse-grained benefits**:
- Reduced memory usage
- Faster conflict checks (fewer locks to scan)
- Automatic cleanup (fewer objects)

#### Callers
- Heap sequential scan (SeqNext)
- Heap index scan (IndexNext)
- Bitmap scan (BitmapHeapNext)

---

## Lock Management Operations

### CheckAndPromotePredicateLockRequest() - Adaptive Promotion

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.82 (memory pressure handling)

#### Signature
```c
static bool CheckAndPromotePredicateLockRequest(
    const PREDICATELOCKTARGETTAG *reqtag)
```

#### Purpose
Determines if requested lock should be immediately promoted to a coarser granularity due to memory constraints. Returns `true` if promoted (caller should not acquire finer-grained lock).

#### Promotion Decision Algorithm

```
if (TotalPredicateLocks > max_predicate_locks) {
    // Global memory pressure
    // Promote to relation-level lock
    return true
}

if (TransactionPredicateLocks > max_predicate_locks_per_transaction) {
    // Per-transaction limit exceeded
    // Promote to coarser granularity
    return true
}

for each existing lock in LocalPredicateLockHash:
    if (ExistingLock can be merged with reqtag) {
        // Can combine with existing lock
        // Promote coarser target
        return true
    }

// No promotion needed
return false
```

#### Coalescing Strategy

When deciding how to coalesce multiple fine-grained locks into coarser ones:

```c
// Example: too many tuple locks on same page
if (LocksOnPage(relation, page) > threshold) {
    // Merge all tuple locks on page → single page lock
    // Release all tuple-level PREDICATELOCK entries
    // Create single page-level PREDICATELOCK entry
}

// Example: too many page locks on same relation
if (LocksOnRelation(relation) > threshold) {
    // Merge all page locks → single relation lock
    // Release all page-level entries
    // Create single relation-level entry
}
```

#### Local Predicate Lock Hash

PostgreSQL maintains a backend-local hash table (`LocalPredicateLockHash`) that tracks locks being held:

```c
typedef struct LOCALPREDICATELOCK {
    PREDICATELOCKTARGETTAG tag;
    bool held;              // Whether actual PREDICATELOCK exists
    int childLocks;         // Count of child locks subsumed
} LOCALPREDICATELOCK;
```

**Purpose**:
- Avoid repeated lookups in shared hash table
- Determine which locks can be coalesced
- Track fine-grained locks that haven't been materialized yet

---

### PredicateLockAcquire() - Core Acquisition

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.85 (atomic lock creation)

#### Signature
```c
static void PredicateLockAcquire(const PREDICATELOCKTARGETTAG *targettag)
```

#### Purpose
Atomically creates a `PREDICATELOCK` entry linking a target to the current transaction.

#### Algorithm

```c
void PredicateLockAcquire(const PREDICATELOCKTARGETTAG *targettag) {
    
    uint32 hashcode = PredicateLockHash(targettag);
    LWLock *partitionlock = 
        PredicateLockHashPartitionLock(hashcode);
    
    // Acquire partition lock
    LWLockAcquire(partitionlock, LW_EXCLUSIVE);
    
    // Check if lock already exists
    existingLock = hash_search_with_hash_value(
        PredicateLockTargetHash,
        targettag, hashcode,
        HASH_FIND, NULL);
    
    if (existingLock == NULL) {
        // Create new target
        newTarget = hash_search_with_hash_value(..., HASH_ENTER, &found);
        dlist_init(&newTarget->predicateLocks);
    }
    
    // Check if this transaction already has lock on target
    for each lock in target->predicateLocks:
        if (lock->tag.myXact == MySerializableXact):
            // Lock already held - done!
            LWLockRelease(partitionlock);
            return;
    
    // Create new PREDICATELOCK entry
    newLock = hash_search(..., HASH_ENTER, ...);
    newLock->tag.myTarget = target;
    newLock->tag.myXact = MySerializableXact;
    
    // Link into target's lock list
    dlist_push_tail(&target->predicateLocks, &newLock->targetLink);
    
    // Link into transaction's lock list
    // (protected by SerializablePredicateListLock)
    dlist_push_tail(&MySerializableXact->predicateLocks, 
                    &newLock->xactLink);
    
    LWLockRelease(partitionlock);
}
```

#### Lock Partition Locking

Multiple partition locks protect the hash table to reduce contention:

```c
#define NUM_PREDICATE_LOCK_PARTITIONS 16

LWLock *PredicateLockHashPartitionLock(uint32 hashcode) {
    return &MainLWLockArray[
        PREDICATE_LOCK_MANAGER_LWLOCK_OFFSET +
        (hashcode % NUM_PREDICATE_LOCK_PARTITIONS)
    ];
}
```

---

### CreatePredicateLock() - Allocation Helper

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.80 (memory allocation)

#### Signature
```c
static void CreatePredicateLock(
    const PREDICATELOCKTARGETTAG *targettag,
    uint32 targettaghash,
    SERIALIZABLEXACT *sxact)
```

#### Purpose
Low-level function that allocates and initializes a `PREDICATELOCK` entry.

#### Implementation
```c
void CreatePredicateLock(const PREDICATELOCKTARGETTAG *targettag,
                         uint32 targettaghash,
                         SERIALIZABLEXACT *sxact) {
    PREDICATELOCKTARGET *target;
    PREDICATELOCK *lock;
    bool found;
    
    // Get or create lock target
    target = hash_search_with_hash_value(
        PredicateLockTargetHash,
        targettag, targettaghash,
        HASH_ENTER, &found);
    
    if (!found) {
        // Initialize new target
        dlist_init(&target->predicateLocks);
    }
    
    // Create lock entry
    lock = (PREDICATELOCK *) malloc(sizeof(PREDICATELOCK));
    lock->tag.myTarget = target;
    lock->tag.myXact = sxact;
    
    dlist_push_tail(&target->predicateLocks, &lock->targetLink);
    dlist_push_tail(&sxact->predicateLocks, &lock->xactLink);
}
```

---

## Lock Transfer and Maintenance

### 1. PredicateLockPageSplit() - B-tree Page Split

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.80 (index maintenance)

#### Signature
```c
void PredicateLockPageSplit(
    Relation relation,
    BlockNumber oldblkno,
    BlockNumber newblkno)
```

#### Purpose
When a B-tree page splits, transfers predicate locks from old page to new page (or keeps on old page, depending on split point).

#### Logic

```c
void PredicateLockPageSplit(Relation relation, 
                           BlockNumber oldblkno,
                           BlockNumber newblkno) {
    
    // Build old and new page tags
    PREDICATELOCKTARGETTAG oldtag = {
        .locktag_field1 = MyDatabaseId,
        .locktag_field2 = relation->rd_id,
        .locktag_field3 = oldblkno,
    };
    
    PREDICATELOCKTARGETTAG newtag = {
        .locktag_field1 = MyDatabaseId,
        .locktag_field2 = relation->rd_id,
        .locktag_field3 = newblkno,
    };
    
    // Transfer locks: when page splits, some data moves to new page
    // Therefore some locks must transfer
    // Decision: keep on old page (don't transfer) because:
    // - Old page had lock, it still has same predicate coverage
    // - Even though some tuples moved, page still represents same predicate range
    
    // Alternative strategy for some cases:
    // - Create lock on new page too (union of lock sets)
    TransferPredicateLocksToNewTarget(oldtag, newtag, false);
}
```

#### Invariant
- After split, lock must cover both old and new pages' predicates
- Conservative: keep lock on old page (already exists)
- Optimization: add lock to new page if its tuple range differs materially

---

### 2. TransferPredicateLocksToHeapRelation() - Index Rebuild

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.82 (index rebuilding)

#### Signature
```c
void TransferPredicateLocksToHeapRelation(Relation relation)
```

#### Purpose
After index rebuild, REINDEX, or similar, transfers all predicate locks from index pages to the base heap relation. Simplifies lock structure after physical reorganization.

#### When Called
- REINDEX command
- Automatic index rebuild during vacuum
- ALTER TABLE CLUSTER

#### Implementation
```c
void TransferPredicateLocksToHeapRelation(Relation relation) {
    
    // Build target tags for index and heap
    PREDICATELOCKTARGETTAG indextag = {..., locktag_field2: index_oid};
    PREDICATELOCKTARGETTAG heaptag = {..., locktag_field2: heap_oid};
    
    // Transfer all predicate locks from index to heap
    // (this is relation-level lock, so covers all index pages)
    
    for each index_lock in all_locks_on_index:
        Transfer(index_lock → heaptag)
}
```

#### Rationale
- Index rebuilding physically reorganizes but doesn't change data
- Tuples still subject to same predicate constraints
- Simplified to track heap rather than volatile index structure

---

### 3. PredicateLockPageCombine() - B-tree Page Combine

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.75 (index maintenance)

#### Signature
```c
void PredicateLockPageCombine(
    Relation relation,
    BlockNumber oldblkno,
    BlockNumber newblkno)
```

#### Purpose
When B-tree pages combine (merge after deletion), consolidates predicate locks.

#### Implementation
```c
// When pages combine:
// - Combine: oldblkno data moved into newblkno
// - Union: if either had predicate lock → both do

PREDICATELOCKTARGETTAG oldtag = {..., oldblkno};
PREDICATELOCKTARGETTAG newtag = {..., newblkno};

// If either page had locks, ensure combined page has lock
if (LockExists(oldtag) || LockExists(newtag)) {
    PredicateLockPage(relation, newblkno);
    // Release old page lock if it exists
    TransferPredicateLocksToNewTarget(oldtag, newtag, true);
}
```

---

## Lock Cleanup and Removal

### ReleasePredicateLocks() - At Transaction End

(See Lifecycle component for details)

### RemoveTargetIfNoLongerUsed()

**Source**: `./src/backend/storage/lmgr/predicate.c`

When removing a lock, check if target object can be deleted:

```c
static void RemoveTargetIfNoLongerUsed(
    PREDICATELOCKTARGET *target,
    uint32 targettaghash) {
    
    // If no locks remain on this target
    if (dlist_is_empty(&target->predicateLocks)) {
        // Remove from hash table
        hash_search(..., HASH_REMOVE, ...);
    }
}
```

---

## Memory Management and Tuning

### GUC Parameters

```c
// Maximum predicate locks per transaction
max_predicate_locks_per_transaction = 64  (default)

// Maximum locks per relation
max_predicate_locks_per_relation = 
    max_predicate_locks_per_transaction / 10

// Maximum total locks
max_predicate_locks = 
    max_connections * max_predicate_locks_per_transaction
```

### Promotion Thresholds

When limits exceeded:
1. First threshold: Fine-grained locks promoted to page locks
2. Second threshold: Page locks promoted to relation locks
3. Third threshold: Entire transaction marked for forced abort

