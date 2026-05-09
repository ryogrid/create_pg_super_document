# PostgreSQL SSI: Synchronization, Shared Memory, and System Integration

## Shared Memory Layout

### Initialization: InitPredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c`

```c
void InitPredicateLocks(void) {
    
    // Allocate shared memory structures
    
    // 1. PredXactList - transaction pool manager
    PredXact = ShmemInitStruct("PredXactListData",
                              sizeof(PredXactListData),
                              &found);
    
    // 2. RWConflictPool - pre-allocated conflict entries
    RWConflictPool = ShmemInitStruct("RWConflictPoolHeaderData",
                                    sizeof(RWConflictPoolHeaderData),
                                    &found);
    
    // 3. PredicateLockTargetHash - lock target hash table
    PredicateLockTargetHash = ShmemInitHash(
        "PredicateLockTargetHash",
        predicate_lock_init_size, predicate_lock_init_size,
        &info, HASH_ELEM | HASH_BLOBS);
    
    // 4. PredicateLockHash - predicate lock entries
    PredicateLockHash = ShmemInitHash(
        "PredicateLockHash",
        predicate_lock_init_size, predicate_lock_init_size,
        &info, HASH_ELEM | HASH_BLOBS);
    
    // 5. SerializableXidHash - xid to sxact mapping
    SerializableXidHash = ShmemInitHash(
        "SerializableXidHash", ..., ...);
    
    // 6. FinishedSerializableTransactions list
    FinishedSerializableTransactions = 
        ShmemInitStruct("FinishedSerializableTransactions", ...);
    
    // 7. SerializableXactHashPartitions - partition locks
    InitPartitionLocks();
    
    // Initialize SLRU for commit history
    SerialInit();
}
```

### Memory Size Calculation

**Signature**:
```c
Size PredicateLockShmemSize(void)
```

**Calculation**:
```c
Size PredicateLockShmemSize(void) {
    Size size = 0;
    
    size += MAXALIGN(sizeof(PredXactListData));
    size += MAXALIGN(sizeof(RWConflictPoolHeaderData));
    
    // Transaction pool
    size += max_connections * sizeof(SERIALIZABLEXACT);
    
    // Conflict pool  
    size += max_connections * (max_connections - 1) 
            * sizeof(RWConflictData);
    
    // Hash tables (estimate)
    size += hash_estimate_size(max_predicate_locks,
                              sizeof(PREDICATELOCKTARGET));
    size += hash_estimate_size(max_predicate_locks,
                              sizeof(PREDICATELOCK));
    size += hash_estimate_size(max_connections * 2,
                              sizeof(SERIALIZABLEXID));
    
    // SLRU for commit history
    size += SimpleLruShmemSize(serializable_buffers, 0);
    
    return MAXALIGN(size);
}
```

---

## Lightweight Lock Hierarchy

### Lock Acquisition Order (Strict)

PostgreSQL enforces acquisition order to prevent deadlocks:

```
1. SerializableFinishedListLock (RW conflict)
   └─ Protects: FinishedSerializableTransactions list
   
2. SerializablePredicateListLock (RW conflict)
   └─ Protects: per-transaction predicate lock list
   
3. SERIALIZABLEXACT->perXactPredicateListLock (RW conflict)
   └─ Protects: same list for parallel workers
   
4. PredicateLockHashPartitionLock[i] (RW conflict)
   └─ Protects: partition i of predicate lock hash table
   └─ Multiple can be held simultaneously (in ascending order)
   
5. SerializableXactHashLock (RW conflict)
   └─ Protects: PredXact->activeList, SerializableXidHash
   
6. SerialControlLock (RW conflict)
   └─ Protects: SerialControlData (SLRU metadata)
   
7. SLRU bank locks (from SimpleLru)
   └─ Protects: SLRU pages for commit history
```

### Partition-Based Locking

```c
// Multiple backends can hold different partition locks
// Reduces contention on critical sections

#define NUM_PREDICATE_LOCK_PARTITIONS 16

// Example: two transactions accessing different relations
// T1: hash(relation1) % 16 = 3 → lock partition 3
// T2: hash(relation2) % 16 = 7 → lock partition 7
// Both can proceed in parallel!
```

---

## Conflict List Management

### Doubly-Linked Lists

All conflict tracking uses PostgreSQL's dlist (doubly-linked list):

```c
typedef struct dlist_node {
    struct dlist_node *prev;
    struct dlist_node *next;
} dlist_node;

typedef struct dlist_head {
    dlist_node head;
} dlist_head;
```

**Used for**:
- Transaction predicate locks: `SERIALIZABLEXACT->predicateLocks`
- Incoming conflicts: `SERIALIZABLEXACT->inConflicts`
- Outgoing conflicts: `SERIALIZABLEXACT->outConflicts`
- Finished transactions: `FinishedSerializableTransactions`

### List Operations

```c
// Add to front (for FIFO)
dlist_push_head(&list, &node->link);

// Add to back (for cleanup LIFO)
dlist_push_tail(&list, &node->link);

// Remove
dlist_delete(&node->link);

// Iterate
for (iter = dlist_foreach_modify(iter, &list)) {
    RWConflict conflict = dlist_container(RWConflictData, 
                                         outLink, iter.cur);
    // Process conflict...
}
```

---

## Local Predicate Lock Hash

### Backend-Local Cache

```c
// In predicate.c: static variable
static HTAB *LocalPredicateLockHash = NULL;

typedef struct LOCALPREDICATELOCK {
    PREDICATELOCKTARGETTAG tag;
    bool held;          // Whether lock actually exists in shared table
    int childLocks;     // Simpler than held for coalescing
} LOCALPREDICATELOCK;
```

### Purpose

```
1. Coalescing Decision: Know which locks we have before
   checking if they should be promoted

2. Avoiding Redundant Checks: Don't repeatedly search
   shared hash table for same target

3. Fine-grained Lock Tracking: Track tuple locks
   even if promoted to page locks
```

### Lifecycle

```c
CreateLocalPredicateLockHash() {
    // Called once per transaction
    // After GetSerializableTransactionSnapshot()
}

// Populated by:
PredicateLockAcquire() {
    // When lock is acquired
    local_hash_insert()
}

ReleasePredicateLocksLocal() {
    // Called during cleanup
    local_hash_destroy()
}
```

---

## Memory Pressure Handling

### Lock Promotion Decision Tree

```
When PredicateLockAcquire(target) called:

1. Count locks in LocalPredicateLockHash
   
   if count > max_predicate_locks_per_transaction:
       → PROMOTE to coarser granularity
       → Return without acquiring

2. Count total shared locks
   
   if total > max_predicate_locks:
       → PROMOTE_GLOBALLY
       → Scan all transactions
       → Promote most locks to page/relation level

3. Count locks on same relation
   
   if locks_on_relation > threshold:
       → PROMOTE to relation lock
       → Release all page and tuple locks
       → Create single relation lock

4. If transaction ran out of local space
   
   → Mark for cleanup sweep
   → Schedule ClearOldPredicateLocks()
```

### Promotion Thresholds

```c
// Fine-grained threshold
FINE_GRAINED_THRESHOLD = max_predicate_locks_per_relation / 2

// Coarse-grained threshold  
COARSE_GRAINED_THRESHOLD = max_predicate_locks_per_relation

// Global threshold
GLOBAL_THRESHOLD = max_predicate_locks
```

---

## Concurrency Patterns

### Scenario 1: Read-Write Conflict Detection

```
Timeline:
T1 (Reader):
  ├─ Acquire snapshot
  ├─ Read predicate locks on relation (page-level)
  └─ Read tuple X
       └─ CheckForSerializableConflictOut(T2_xid)

T2 (Writer):
  ├─ Write tuple X
  └─ CheckForSerializableConflictIn(T1_locks)
      └─ FlagRWConflict(T1, T2)

Lock Protection:
- T1 reading: partition lock (shared) for predicate locks
- T2 writing: partition lock (exclusive) for conflicts
- No deadlock: T1 releases before T2 acquires
```

### Scenario 2: Dangerous Structure Detection

```
T1 (Writer):           T2 (Pivot):            T3 (Reader):
─────────────────────────────────────────────────────────
Create lock T1→T2     Create lock T2→T3       
OnConflict_Check()    OnConflict_Check()
(patterns checked)    (patterns checked)
                      
Pattern detected at T2.outConflicts! (T2→T3)
Dangerous: T1→T2→T3
Decision: MarkSxactDoomed(T2)

Lock Sequence:
  T1 acquires: SerializableFinishedListLock → partition locks
  T2 updates:  Same locks in same order
  T3 checks:   Same locks in same order
  → No deadlock via strict ordering
```

---

## Parallel Query Integration

### Leader-Worker Coordination

```c
// Leader (parent transaction):
MySerializableXact = CreatePredXact();

// Worker (parallel subprocess):
SetSerializableTransactionSnapshot(snapshot, 
    parent_vxid, parent_pid) {
    
    // Worker attaches to parent's transaction
    MySerializableXact = parent_sxact;
    
    // Lock: perXactPredicateListLock
    // Why: leader and worker might modify list simultaneously
}

// Cleanup sequence:
Leader finishes:
├─ ReleasePredicateLocks(true, false) for main path
├─ Workers already exited
└─ If workers still active:
   └─ ReleaseOneSerializableXact(partial=true)
      └─ Mark PARTIALLY_RELEASED
      
Later at END-OF-TRANSACTION:
└─ ReleaseOneSerializableXact(partial=false)
   └─ Final cleanup
```

---

## Statistics and Monitoring

### Debug Mode

```c
#ifdef PREDICATE_LOCK_DEBUG
    // Extensive logging of lock operations
    #define PREDICATE_PRINT(fmt, ...)  ereport(DEBUG2, ...)
#else
    #define PREDICATE_PRINT(fmt, ...)  ((void) 0)
#endif
```

### Metrics Tracked

```c
// In SerialControlData and PredXactListData:
struct {
    int TotalPredicateLocks;        // Current count
    int MaxPredicateLocks;          // Peak count
    int PromotionsFromTuple;        // # tuple→page promotions
    int PromotionsFromPage;         // # page→relation promotions
    int DangerousStructuresDetected; // # times pattern found
    int SerializationFailures;      // # transactions aborted
    int RoSafeTransactions;         // # read-only safe commits
} PredLockStats;
```

---

## Integration with Backend Startup

### In PostmasterMain()

```c
InitializeGUCOptions() {
    // Load GUC parameters
    max_predicate_locks
    max_predicate_locks_per_transaction
    serializable_buffers
}

CreateSharedMemoryAndSemaphores() {
    // Calculate required size
    shmem_size += PredicateLockShmemSize();
    
    // Allocate
    shmem = shmalloc(shmem_size);
}

InitPredicateLocks() {
    // Initialize shared structures
}
```

### Per-Backend Initialization

```c
InitBackendVars() {
    MySerializableXact = InvalidSerializableXact;
    MyXactDidWrite = false;
    LocalPredicateLockHash = NULL;
    SavedSerializableXact = InvalidSerializableXact;
}
```

---

## Cleanup and Shutdown

### At Backend Exit

```c
proc_exit(code) {
    // If transaction active and serializable:
    if (MySerializableXact != InvalidSerializableXact) {
        
        if (xact_state == TRANS_INABORT):
            ReleasePredicateLocks(false, false);
        // else: already released at commit
    }
    
    // Destroy local hash
    ReleasePredicateLocksLocal();
}
```

### At Postmaster Shutdown

```c
PostmasterShutdown(code) {
    // SLRU flush
    SimpleLruFlush(SerialSlruCtl, true);
    
    // Shared memory automatically cleaned up
}
```

