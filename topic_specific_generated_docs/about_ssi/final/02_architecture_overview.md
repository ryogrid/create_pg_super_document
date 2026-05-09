# Architecture Overview: PostgreSQL SSI System Design

## System-Wide Perspective

Serializable Snapshot Isolation (SSI) operates at the intersection of PostgreSQL's transaction manager, snapshot manager, and predicate locking subsystem. This chapter provides a complete architectural overview of how these components interact to provide serializable isolation.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Backend Process                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              SQL Query Execution Engine                  │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │    Parser    │  │   Planner    │  │   Executor   │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  │         │                 │                 │            │    │
│  └─────────┼─────────────────┼─────────────────┼────────────┘    │
│            │                 │                 │                  │
│            └─────────────────┼─────────────────┘                  │
│                              │                                    │
│                    ┌─────────▼────────┐                           │
│                    │ Data Access Layer │                           │
│                    │ (Heap, Index Scans)                          │
│                    └─────────┬────────┘                           │
│                              │                                    │
│                    ┌─────────▼────────────────────┐               │
│                    │  SSI Transaction Lifecycle   │               │
│                    │  ┌──────────────────────┐    │               │
│                    │  │ GetSerializableTxnSnap│   │               │
│                    │  │ (Entry Point)        │    │               │
│                    │  └──────────────────────┘    │               │
│                    └─────────┬────────────────────┘               │
│                              │                                    │
│            ┌─────────────────┼──────────────────┐                 │
│            │                 │                  │                 │
│    ┌───────▼──────┐  ┌──────▼──────┐  ┌───────▼──────┐           │
│    │   Predicate  │  │  Conflict   │  │   Commit     │           │
│    │   Locking    │  │  Detection  │  │  Validation  │           │
│    │   Subsystem  │  │  Subsystem  │  │  Subsystem   │           │
│    └───────┬──────┘  └──────┬──────┘  └───────┬──────┘           │
│            │                │                 │                   │
│            └────────────────┼─────────────────┘                   │
│                             │                                    │
│                    ┌────────▼─────────┐                          │
│                    │  Shared Memory   │                          │
│                    │  State Tracking  │                          │
│                    │  (LWLocks)       │                          │
│                    └────────┬─────────┘                          │
│                             │                                    │
│            ┌────────────────┴────────────────┐                   │
│            │                                  │                   │
│    ┌───────▼────────┐            ┌──────────▼────────┐           │
│    │  Predicate     │            │   Conflict       │           │
│    │  Lock Hash     │            │   Graph Hash     │           │
│    │  Tables        │            │   Tables         │           │
│    │ (SHMEM)        │            │  (SHMEM)         │           │
│    └────────────────┘            └──────────────────┘           │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

### 1. Transaction Lifecycle Manager
**Responsibility**: Coordinate transaction entry points and integrate with xact.c  
**Key Functions**:
- `GetSerializableTransactionSnapshot()` - Primary entry point
- `RegisterPredicateLockingXid()` - Create SERIALIZABLEXACT
- `PreCommit_CheckForSerializationFailure()` - Validate at commit
- `ReleasePredicateLocks()` - Cleanup

**Integration Points**:
- `xact.c:GetTransactionSnapshot()` - Integrates for SERIALIZABLE transactions
- `xact.c:CommitTransaction()` - Pre-commit checks
- `xact.c:AbortTransaction()` - Cleanup on abort

### 2. Predicate Locking Subsystem
**Responsibility**: Acquire, track, and manage predicate locks (SIREAD locks)  
**Key Functions**:
- `PredicateLockRelation()` - Lock entire table
- `PredicateLockPage()` - Lock 8KB page
- `PredicateLockTuple()` - Lock single row
- `PredicateLockAcquire()` - Core lock acquisition with promotion heuristic

**Data Structures**:
- `PREDICATELOCK` - Individual lock representation
- `PREDICATELOCKTARGET` - Target being locked
- `LOCALPREDICATELOCK` - Per-transaction lock tracking

### 3. Conflict Detection Subsystem
**Responsibility**: Detect read-write conflicts and build conflict graph  
**Key Functions**:
- `CheckForSerializableConflictOut()` - Detect conflicts on reads
- `CheckForSerializableConflictIn()` - Detect conflicts on writes
- `OnConflict_CheckForSerializationFailure()` - Dangerous structure detection

**Data Structures**:
- `RWConflict` - Directed edge in conflict graph
- `RWConflictPool` - Memory pool for conflict objects

### 4. Commit Validation Subsystem
**Responsibility**: Perform final serialization checks before commit  
**Key Functions**:
- `PreCommit_CheckForSerializationFailure()` - Entry point
- `SummarizeOldestCommittedSxact()` - Compress old transaction state

**Algorithms**:
- Dangerous structure detection (Tin-Tpivot-Tout)
- Safe snapshot verification for RO txns

### 5. Shared Memory Manager
**Responsibility**: Allocate and protect SSI state in SHMEM  
**Key Structures**:
- `SerialControlData` - Global SSI control block
- `PREDICATELOCKTAG` hash tables
- `SERIALIZABLEXID` hash tables
- Conflict graph edge pools

**Synchronization**:
- `SerializableXactHashLock` - Protects transaction hash table
- `PredicateLockHashLock` - Protects predicate lock hash table
- Partition locks for scalability (reduces contention)

## Data Flow: A Complete Transaction

```
1. Transaction Start
   ├─ SetTransactionIsolationLevel() [user app]
   ├─ SetIsoLevel = SERIALIZABLE
   └─ BEGIN TRANSACTION

2. Snapshot Acquisition
   ├─ GetTransactionSnapshot() [from snapmgr.c]
   ├─ GetSerializableTransactionSnapshot() [SSI entry point]
   ├─ Create SERIALIZABLEXACT object [shared memory]
   ├─ GetSafeSnapshot() [for read-only, deferred]
   └─ Return snapshot to executor

3. First Read Operation
   ├─ Executor calls heap/index scan
   ├─ Tuple visibility check using snapshot
   ├─ CheckForSerializableConflictOut()
   │  └─ Find prior writes to same predicate
   │     └─ Create conflict edges in graph
   └─ Return data to executor

4. Write Operation
   ├─ Executor calls heap_insert/update/delete
   ├─ PredicateLockRelation() [or Page/Tuple]
   │  ├─ Check memory pressure
   │  ├─ Coalesce if max_predicate_locks exceeded
   │  └─ Add lock to shared memory hash table
   ├─ CheckForSerializableConflictIn()
   │  ├─ Find prior reads in other txns
   │  ├─ Create conflict edges
   │  └─ Check for dangerous structures
   └─ Continue execution

5. Commit Path
   ├─ CommitTransaction() [xact.c]
   ├─ PreCommit_CheckForSerializationFailure()
   │  ├─ Full dangerous structure scan
   │  ├─ Check for Tin-Tpivot-Tout pattern
   │  └─ Decide: commit, abort, or defer
   ├─ If conflict found
   │  └─ Raise SERIALIZATION_FAILURE exception
   ├─ ReleasePredicateLocks() [cleanup]
   └─ Transaction ends

6. Abort Path (if conflict)
   ├─ AbortTransaction() [xact.c]
   ├─ ReleasePredicateLocks() [cleanup]
   └─ Exception propagated to application
       (must retry transaction)
```

## Shared Memory State Organization

SSI maintains persistent state in PostgreSQL's shared memory:

```
SHMEM Layout (Simplified)
├─ SerialControlData (1 block)
│  ├─ Global xmin tracking
│  ├─ SHMEM allocation pointers
│  └─ LWLock definitions
│
├─ SERIALIZABLEXID Hash Table (1 hash)
│  ├─ Active transactions
│  ├─ Recently committed txns
│  └─ Summarized old txns
│
├─ PREDICATELOCKTAG Hash Table (1 hash)
│  ├─ All active predicate locks
│  ├─ Grouped by lock target
│  └─ Linked to PREDICATELOCK objects
│
├─ PREDICATELOCK Array (bounded)
│  ├─ Individual lock objects
│  └─ Limited by max_predicate_locks
│
└─ Conflict Pool (bounded)
   ├─ RWConflict edges
   ├─ Linked list of conflicts
   └─ Limited by available memory
```

**Memory Management Strategy**:
- Pre-allocated at server startup (size based on `max_predicate_locks`)
- When full, predicate lock promotion triggered
- Oldest committed transactions summarized to SLRU
- Provides bounded, predictable memory usage

## Integration with PostgreSQL Core

### Transaction Manager (`xact.c`)
- Calls `GetSerializableTransactionSnapshot()` for SERIALIZABLE isolation
- Calls `PreCommit_CheckForSerializationFailure()` before actual commit
- Calls `ReleasePredicateLocks()` during cleanup

### Snapshot Manager (`snapmgr.c`)
- Detects isolation level
- Delegates to SSI for serializable snapshots
- Uses returned snapshot for transaction visibility

### Executor (`executor/*)
- Calls `CheckForSerializableConflictOut()` on tuple visibility
- Calls `CheckForSerializableConflictIn()` on data modification
- Propagates conflicts to SSI subsystem

### Access Methods (`heap.c`, `nbtree.c`, etc.)
- Report accessed predicates (relation, page, tuple)
- Trigger predicate lock acquisition
- Handle lock transfers on page splits/combines

### MVCC Layer (`tqual.c`)
- Snapshot-based visibility checking
- Integrates conflict detection with tuple visibility
- Handles xmax/xmin for conflict determination

## Performance Characteristics

### Lock Acquisition Path
```
PredicateLockAcquire()
├─ Hash lookup for existing lock
├─ If exists: return (fast path)
├─ If not exists
│  ├─ Check memory (coalesce if needed)
│  ├─ Allocate lock object
│  ├─ Insert in hash table
│  └─ Insert in LOCALPREDICATELOCK list
└─ Cost: O(1) expected, with lock coalescing heuristic
```

### Conflict Detection Path
```
CheckForSerializableConflictOut()
├─ For each matching PREDICATELOCK
├─ For each txn holding that lock
├─ Check for prior write conflict
│  └─ If found: create RWConflict edge
└─ Cost: O(n) where n = txns holding overlapping locks
```

### Commit Validation Path
```
PreCommit_CheckForSerializationFailure()
├─ If read-only and safe snapshot
│  └─ Return immediately (O(1))
├─ If write txn
│  ├─ Full dangerous structure scan
│  └─ Cost: O(E) where E = conflict edges
└─ Average case: O(1), worst case: O(n+E)
```

## Synchronization Strategy

SSI uses fine-grained locking to maintain scalability:

| Lock | Protects | Held By | Contention |
|------|----------|---------|-----------|
| `SerializableXactHashLock` | Transaction state hash table | Transaction lifecycle ops | Medium (brief) |
| `PredicateLockHashLock` | Predicate lock hash table | Lock acquisition | Medium (brief) |
| `SerializableFinishedListLock` | Recently committed list | Cleanup/summarization | Low (rare) |
| Partition locks | PREDICATELOCKTAG buckets | Lock-specific ops | Low (distributed) |

**Lock Ordering** (to prevent deadlock):
1. Acquire on separate txn only if necessary
2. Always acquire locks in this order: `SerializableXactHashLock` → `PredicateLockHashLock` → partition locks

## Design Patterns

### Pattern 1: Lock Coalescing
When memory is constrained, multiple fine-grained locks are combined into coarser locks:
- 64 tuple locks on same page → 1 page lock
- Many page locks on same table → 1 relation lock

**Trade-off**: Reduces memory but increases false positives (more potential conflicts)

### Pattern 2: Dangerous Structure Detection
Three-transaction cycle pattern identified to prevent serializability violations:
- Tin writes, Tpivot reads (creates WR conflict)
- Tpivot writes, Tout reads (creates WR conflict)
- Tout writes, Tin reads (would create WR conflict, creates cycle)

**Decision**: Abort one txn before cycle completes

### Pattern 3: Safe Snapshot Optimization
Read-only transactions with "safe snapshots" can bypass all conflict checking:
- Safe snapshot = no concurrent writes to tuples read
- Detected by tracking read predicates + concurrent writes
- Enables zero-overhead RO transactions

### Pattern 4: Summarization
Old completed transactions compressed to SLRU (Simplified Least Recently Used):
- Aggregates multiple committed txns into summary records
- Reduces memory usage as transactions complete
- Maintains just enough info for future conflict detection

---

## Key Insights

1. **Snapshot-Based**: Readers use MVCC snapshots, never block
2. **Predicate-Based**: Locks on data ranges, not physical rows
3. **Proactive Detection**: Identifies conflicts before they cause anomalies
4. **Scalable**: Memory-bounded, partition locks reduce contention
5. **Integrated**: Works within PostgreSQL's existing transaction infrastructure

---

## Prerequisites
- Understanding of PostgreSQL transaction isolation levels
- Familiarity with MVCC (Multi-Version Concurrency Control)
- Basic knowledge of lock-based concurrency control

## Next Steps
→ [Lifecycle and Entry Points](03_lifecycle_and_entry_points.md) - Understand transaction entry points  
→ [Predicate Locking](05_predicate_locking.md) - Learn lock acquisition and promotion  
→ [Deep Dives](18_deep_dives.md) - Explore dangerous structure algorithm in detail
