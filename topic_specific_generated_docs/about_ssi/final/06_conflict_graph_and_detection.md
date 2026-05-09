# PostgreSQL SSI: Conflict Graph and Dangerous Structure Detection

## Overview

The heart of SSI lies in detecting the "dangerous structure" - a specific pattern of read-write conflicts that indicates potential for serialization anomalies. Instead of tracking all dependencies like 2PL, SSI monitors only rw-conflicts (one transaction reads what another writes) and detects the specific 3-transaction pattern: Tin → Tpivot → Tout.

**Key Insight**: Not all rw-conflict cycles cause anomalies. Only cycles containing a "dangerous structure" indicate problems. This reduces false positives and overhead compared to full cycle detection.

## RW-Conflict Graph Fundamentals

### What is an RW-Conflict?

An rw-conflict occurs when:
1. **Transaction A (reader)** reads a database object
2. **Transaction B (writer)** concurrently writes that object
3. **A's snapshot** doesn't include B's write (B is concurrent)

```
Timeline:
A: START SNAP ----READ---- COMMIT
         B: WRITE
         
Result: rw-conflict: B → A
        (B writes, A reads the uncommitted version doesn't see it,
         but A has conflict tracking B)
```

### Conflict Direction

- **Conflict OUT**: T has written data that another transaction reads
- **Conflict IN**: T has read data that another transaction writes later

```c
typedef struct RWConflictData {
    SERIALIZABLEXACT *sxactOut;  // Writer (has conflict out)
    SERIALIZABLEXACT *sxactIn;   // Reader (has conflict in)
    dlist_node outLink;          // Link in writer's outConflicts
    dlist_node inLink;           // Link in reader's inConflicts
} RWConflictData;
```

Invariant: `sxactOut != sxactIn`

### Dangerous Structure Pattern

The fundamental theorem (Cahill et al. 2008):

**Every isolation anomaly corresponds to a cycle containing at least one dangerous structure:**

```
      Tin -------> Tpivot ------> Tout
           rw-conflict    rw-conflict
```

Where:
- `Tin` has rw-conflict **out** to `Tpivot` (Tin writes → Tpivot reads)
- `Tpivot` has rw-conflict **out** to `Tout` (Tpivot writes → Tout reads)
- Some `Tout` later has rw-conflict **out** to something that eventually cycles back

**Why this matters**:
- Only need to detect this 3-vertex pattern
- Don't need to track wr- and ww-dependencies
- Dramatically reduces overhead vs. 2PL
- False positives possible (not all patterns embed in cycles) but rare

---

## Core Conflict Detection APIs

### 1. CheckForSerializableConflictOut() - Read Conflict Detection

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.95 (critical for read detection)

#### Signature
```c
void CheckForSerializableConflictOut(
    Relation relation,
    TransactionId xid,
    Snapshot snapshot)
```

#### Parameters
- `relation` (Relation*): Table being read
- `xid` (TransactionId): XID that wrote the tuple being read
- `snapshot` (Snapshot): Current transaction's snapshot

#### Purpose
Called when reading a tuple written by concurrent transaction. Checks if the writer is serializable and creates rw-conflict record if so.

#### Algorithm

```
function CheckForSerializableConflictOut(relation, xid, snapshot):
    
    if NOT SerializationNeededForRead(relation, snapshot):
        return  // No SSI for this relation or snapshot
    
    // Look up whether the writing transaction is serializable
    writingSxact = GetSerializableTransaction(xid)
    
    if writingSxact == NULL:
        return  // Writer not using SSI, no conflict
    
    if SxactIsDoomed(writingSxact):
        return  // Writer already being aborted
    
    if SxactIsROSafe(MySerializableXact):
        return  // Our transaction is RO-safe, no conflicts possible
    
    // Check if conflict already recorded
    if RWConflictExists(MySerializableXact, writingSxact):
        return  // Already have this conflict
    
    // Create new rw-conflict
    SetRWConflict(MySerializableXact, writingSxact)
    
    // Mark if this creates a dangerous structure
    OnConflict_CheckForSerializationFailure(MySerializableXact, 
                                            writingSxact)
```

#### Caller Context

Called from heap scan routines when tuple is visible:

```c
// In heapam.c heap_getpage()
for each tuple in page:
    if HeapTupleSatisfiesMVCC(tuple, snapshot):
        if SERIALIZABLE_ISOLATION():
            CheckForSerializableConflictOut(
                relation,
                HeapTupleHeaderGetXmin(tuple->t_data),
                snapshot)
        // Process tuple...
```

#### When NOT Called

1. **Tuple too old**: `xid < snapshot->xmin` (committed before snapshot)
2. **Reading own write**: `xid == MyTopTransactionId`
3. **Not serializable**: `IsolationLevel != SERIALIZABLE`
4. **Special snapshot**: Cluster/reindex use non-MVCC snapshots

#### Performance Characteristics

- **Time**: O(1) for conflict existence check, O(1) for conflict creation
- **Memory**: O(1) per conflict (from RWConflictPool)
- **Frequency**: Called once per visible tuple in scan

---

### 2. CheckForSerializableConflictIn() - Write Conflict Detection

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.95 (critical for write detection)

#### Signature
```c
void CheckForSerializableConflictIn(
    Relation relation,
    ItemPointer tid,
    BlockNumber blkno)
```

#### Parameters
- `relation` (Relation*): Table being written to
- `tid` (ItemPointer): Tuple ID being modified
- `blkno` (BlockNumber): Block number (for page-level conflicts)

#### Purpose
Called when **writing** a tuple. Checks if any other serializable transaction has a **predicate lock** covering this write, indicating they read data this write could affect.

#### Algorithm

```
function CheckForSerializableConflictIn(relation, tid, blkno):
    
    if NOT SerializationNeededForWrite(relation):
        return
    
    // Mark our transaction as having done a write
    MyXactDidWrite = true
    
    // Ensure our transaction record exists (for conflict tracking)
    RegisterPredicateLockingXid()
    
    // Check predicate locks at three granularities
    
    // 1. Tuple-level locks
    tupleTag = {dboid, relid, blockno, offsetno}
    CheckTargetForConflictsIn(&tupleTag)
    
    // 2. Page-level locks (covers this page)
    pageTag = {dboid, relid, blockno, InvalidOffset}
    CheckTargetForConflictsIn(&pageTag)
    
    // 3. Relation-level locks (covers entire table)
    relationTag = {dboid, relid, InvalidBlockNum, 0}
    CheckTargetForConflictsIn(&relationTag)
```

#### CheckTargetForConflictsIn() - Target Conflict Check

**Source**: `./src/backend/storage/lmgr/predicate.c`

```c
static void CheckTargetForConflictsIn(
    PREDICATELOCKTARGETTAG *targettag) {
    
    PREDICATELOCKTARGET *target;
    PREDICATELOCK *lock;
    
    // Look up locks on this target
    target = hash_search(PredicateLockTargetHash, 
                        targettag, HASH_FIND, NULL);
    
    if (target == NULL)
        return;  // No locks on this target
    
    // Check each lock on this target
    for each lock in target->predicateLocks:
        
        readingSxact = lock->tag.myXact
        
        if readingSxact == MySerializableXact:
            continue  // Ignore own locks
        
        if SxactIsDoomed(readingSxact):
            continue  // Ignore doomed transactions
        
        // Found a conflict! 
        // Serializable transaction locked this target,
        // we're writing it - rw-conflict!
        
        FlagRWConflict(readingSxact, MySerializableXact)
        OnConflict_CheckForSerializationFailure(
            readingSxact,
            MySerializableXact)
}
```

#### Caller Context

Called from INSERT/UPDATE/DELETE:

```c
// In heapam.c heap_insert(), heap_update(), heap_delete()
heap_insert(relation, tuple, ...) {
    
    // ... perform insert ...
    
    // Check for conflicts with readers
    CheckForSerializableConflictIn(relation, &tuple->t_self, 
                                  ItemPointerGetBlockNumber(&tuple->t_self))
}
```

---

### 3. CheckTableForSerializableConflictIn() - Bulk Operations

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.85 (DDL integration)

#### Signature
```c
void CheckTableForSerializableConflictIn(Relation relation)
```

#### Purpose
For bulk operations (TRUNCATE, CLUSTER, VACUUM) that affect entire table, check all locks on table at once.

#### Implementation

```c
void CheckTableForSerializableConflictIn(Relation relation) {
    
    // Build relation-level lock tag
    PREDICATELOCKTARGETTAG tag = {
        MyDatabaseId, relation->rd_id, InvalidBlockNumber, 0
    };
    
    // This is equivalent to: scan all tuples and call
    // CheckForSerializableConflictIn on each
    // But optimized: one pass through predicate locks
    
    CheckTargetForConflictsIn(&tag);
}
```

#### Called From
- `TRUNCATE` command: `relation.c:truncate_check_rel()`
- `VACUUM`: `vacuumlazy.c:heapam_relation_needs_toast_table()`
- `CLUSTER`: `cluster.c:rebuild_relation()`
- Index rebuild: `index.c:index_build()`

---

## Dangerous Structure Detection

### 1. OnConflict_CheckForSerializationFailure() - Core Detection

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.98 (THE critical function)

#### Signature
```c
static void OnConflict_CheckForSerializationFailure(
    const SERIALIZABLEXACT *reader,
    SERIALIZABLEXACT *writer)
```

#### Purpose
**This is the core SSI algorithm.** Called whenever rw-conflict is created. Searches for dangerous structure pattern and decides which transaction to abort.

#### Parameters
- `reader` (const SERIALIZABLEXACT*): Transaction with conflict in (reading)
- `writer` (SERIALIZABLEXACT*): Transaction with conflict out (writing)

#### Algorithm - Dangerous Structure Detection

```
function OnConflict_CheckForSerializationFailure(reader, writer):
    
    // We just created: reader --rw-conflict-in--> writer
    // This is: writer wrote → reader read
    
    // Pattern we're looking for:
    //    Tin ----rw---> Tpivot ----rw---> Tout
    
    // Current situation:
    //    Tin/reader ----rw---> writer
    // 
    // So: reader is Tin, writer is Tpivot
    // Now look for: writer ----rw---> Tout
    //              (tpivot has conflict out)
    
    // Case 1: Check if writer could have conflict out
    // (i.e., writer already has conflict out to another transaction)
    
    for each Tout in writer->outConflicts:
        
        // Found dangerous structure!
        // Pattern: reader -> writer -> Tout
        
        // Decision: who to abort?
        // Rule 1: Tout must commit before being rolled back
        
        if SxactIsCommitted(Tout):
            // Tout has already committed
            // According to SSI proof, this creates a real cycle
            // Must abort someone: prioritize aborting writer
            
            MarkSxactDoomed(writer)
            // writer will be aborted at commit time
            
        else if not SxactIsDoomed(Tout):
            // Tout is still active, hasn't committed
            // Mark for potential abort, but defer decision
            // Wait for Tout to commit first
            
            // But don't abort writer YET
            // Tout might abort on its own
```

#### Full Pseudocode with Optimization

PostgreSQL includes important optimizations based on Cahill's theorem:

**Optimization 1: Cout must commit before cycle**
```
Don't rollback unless Tout has already committed.
This ensures we're detecting a real cycle.
```

**Optimization 2: Read-only transactions safe**
```
if reader is read-only:
    // No edge reader -> X possible
    // (read-only transactions never write)
    // Cycle impossible!
    return  // No serialization failure
```

**Optimization 3: CommitSeqNo ordering**
```
if (writer->prepareSeqNo < reader->lastCommitBeforeSnapshot):
    // Writer prepared after reader started
    // But reader's snapshot predates writer's commit
    // Specific ordering prevents cycle
    return
```

#### Decision Logic: Who to Abort?

When dangerous structure detected with Tout already committed:

```
Priority for abort:
1. writer (Tpivot) - has rw-conflicts both in and out
2. reader (Tin) - if writer can't be aborted
3. Tout - least preferred (already committed)

Actual code:
if can_abort(writer) and not SxactIsCommitted(writer):
    MarkSxactDoomed(writer)
elif can_abort(reader) and not SxactIsCommitted(reader):
    MarkSxactDoomed(reader)
else:
    // Last resort: abort Tout after it commits
    MarkSxactDoomed(Tout)
```

---

### 2. FlagRWConflict() - Conflict Creation

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.90 (prerequisite to detection)

#### Signature
```c
static void FlagRWConflict(
    SERIALIZABLEXACT *reader,
    SERIALIZABLEXACT *writer)
```

#### Purpose
Creates bidirectional rw-conflict record linking reader and writer transactions.

#### Implementation

```c
void FlagRWConflict(SERIALIZABLEXACT *reader,
                    SERIALIZABLEXACT *writer) {
    
    Assert(reader != writer);
    
    // Use pool of pre-allocated RWConflictData objects
    if (dlist_is_empty(&RWConflictPool->availableList)) {
        ereport(ERROR,
            (errmsg("not enough RWConflict pool entries")));
    }
    
    // Get conflict from pool
    conflict = dlist_pop_head_element(RWConflictData, outLink,
                                      &RWConflictPool->availableList);
    
    // Initialize
    conflict->sxactOut = writer;
    conflict->sxactIn = reader;
    
    // Link into reader's incoming conflicts
    dlist_push_tail(&reader->inConflicts, &conflict->inLink);
    
    // Link into writer's outgoing conflicts
    dlist_push_tail(&writer->outConflicts, &conflict->outLink);
}
```

#### RWConflictPool Management

Pre-allocates pool at startup:

```
Pool Size = 
    (max_connections) * (max_connections - 1) * CONFLICT_POOL_FACTOR
    
= enough for each pair of transactions to have multiple conflicts
```

---

### 3. RWConflictExists() - Idempotency Check

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.80 (prevents duplicate conflicts)

#### Signature
```c
static bool RWConflictExists(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer)
```

#### Purpose
Checks if rw-conflict already recorded between reader and writer, to avoid creating duplicates.

#### Implementation

```c
bool RWConflictExists(const SERIALIZABLEXACT *reader,
                      const SERIALIZABLEXACT *writer) {
    
    if SxactIsDoomed(reader) || SxactIsDoomed(writer):
        return false  // Doomed xacts don't matter
    
    if dlist_is_empty(&reader->outConflicts):
        return false
    
    // Search reader's outgoing conflicts
    for each conflict in reader->outConflicts:
        if conflict->sxactIn == writer:
            return true  // Found it!
    
    return false
}
```

---

## SetPossibleUnsafeConflict() - Read-Only Safety Tracking

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.80 (read-only optimization)

#### Signature
```c
static void SetPossibleUnsafeConflict(
    SERIALIZABLEXACT *roXact,
    SERIALIZABLEXACT *activeXact)
```

#### Purpose
For read-only transactions, track potential conflicts with active R/W transactions to determine if snapshot is "safe".

#### Semantics

```
roXact has possibleUnsafeConflict with activeXact if:
- roXact is read-only
- activeXact is read-write
- roXact and activeXact overlap in time
- Haven't yet verified that activeXact won't cause serialization issue

Once activeXact commits without conflict → can be removed
If activeXact creates real conflict → roXact is UNSAFE
```

#### Implementation

```c
void SetPossibleUnsafeConflict(SERIALIZABLEXACT *roXact,
                               SERIALIZABLEXACT *activeXact) {
    
    Assert(SxactIsReadOnly(roXact));
    Assert(!SxactIsReadOnly(activeXact));
    
    // Use same RWConflictData structure
    // (reusing for possible vs. actual conflicts)
    
    if (dlist_is_empty(&RWConflictPool->availableList)) {
        ereport(ERROR, ...);
    }
    
    conflict = dlist_pop_head_element(...);
    
    // Direction: activeXact -> roXact (potential)
    conflict->sxactOut = activeXact;
    conflict->sxactIn = roXact;
    
    // Link into both transaction lists
    dlist_push_tail(&activeXact->possibleUnsafeConflicts, 
                    &conflict->outLink);
    dlist_push_tail(&roXact->possibleUnsafeConflicts,
                    &conflict->inLink);
}
```

---

## Graph Visualization

### Conflict Graph Structure

```
Active Transactions in Memory
┌─────────────────────────────────────────┐
│ T1 (RW) ──→ T2 (RO) ──→ T3 (RW) ──→ T1  │
│   \            |           /           │
│    ──conflicts from/to──                │
│                                          │
│ Dangerous Structure Detected!           │
│ T1 ──rw→ T2 ──rw→ T3                   │
│  └─ real cycle after T3 commits ─→     │
└─────────────────────────────────────────┘

Decision: Abort T2 (Tpivot in pattern)
Reason: Has conflicts both in and out
```

### Memory Layout

```
SERIALIZABLEXACT (T1)
├── outConflicts ──→ [T2 (writer writes, T1 reads)]
├── inConflicts
└── possibleUnsafeConflicts

SERIALIZABLEXACT (T2)
├── outConflicts ──→ [T3, T4, ...]
├── inConflicts ──→ [T1]
└── possibleUnsafeConflicts

SERIALIZABLEXACT (T3)
├── outConflicts
├── inConflicts ──→ [T2]
└── possibleUnsafeConflicts
```

---

## Integration Points

### With Tuple Visibility (heapam.c)

```c
if (SerializationNeededForRead(relation, snapshot)) {
    // After determining tuple is visible
    CheckForSerializableConflictOut(
        relation,
        HeapTupleHeaderGetXmin(tuple->t_data),
        snapshot);
}
```

### With Write Operations (heapam.c)

```c
// Before inserting/updating/deleting
CheckForSerializableConflictIn(relation, tid, blkno);

// Perform the write...
```

### With Predicate Locking

Predicate locks determine **scope** of conflict detection:
- Relation lock → check all writers to table
- Page lock → check all writers to page
- Tuple lock → check writer to specific tuple


---

## Prerequisites
- Complete understanding of all prior chapters (especially Chapter 05)
- Familiarity with PostgreSQL transaction isolation and MVCC
- Understanding of shared memory and LWLock synchronization

## Next Steps
→ [Chapter 7: 07 *](../final/07_*.md)
→ [Back to Architecture Overview](02_architecture_overview.md)
→ [Jump to Deep Dives](18_deep_dives.md) for advanced topics
