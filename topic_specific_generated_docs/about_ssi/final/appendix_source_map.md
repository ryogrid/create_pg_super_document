# Appendix C: Source Map

**Comprehensive file and function mapping for PostgreSQL SSI implementation.**

---

## PostgreSQL Source Directory Structure

```
./src/
├── backend/
│   ├── storage/
│   │   └── lmgr/
│   │       ├── predicate.c                    (5053 lines - MAIN)
│   │       ├── lock.c                         (1200+ lines - regular locks)
│   │       └── README-SSI                     (900 lines - conceptual docs)
│   ├── executor/
│   │   ├── execUtils.c                        (integration points)
│   │   ├── nodeSeqscan.c                      (→ PredicateLockRelation)
│   │   └── nodeIndexscan.c                    (→ PredicateLockPage/Tuple)
│   ├── access/
│   │   ├── heap/
│   │   │   ├── heapam.c                       (INSERT/UPDATE/DELETE hooks)
│   │   │   └── heapam_handler.c               (heap access method)
│   │   └── index/
│   │       ├── nbtree.c                       (B-tree integration)
│   │       └── indexam.c                      (index scan integration)
│   ├── tcop/
│   │   └── xact.c                             (→ GetSerializableTransactionSnapshot)
│   └── utils/
│       ├── mmgr/
│       │   └── shmem.c                        (shared memory allocation)
│       └── time/
│           └── snapmgr.c                      (→ GetTransactionSnapshot)
│
└── include/
    └── storage/
        ├── predicate.h                        (52 lines - public API)
        ├── predicate_internals.h              (400 lines - internal structures)
        ├── lock.h                             (common lock infrastructure)
        ├── bufmgr.h                           (buffer cache integration)
        └── xact.h                             (transaction state)
```

---

## Key Files and Their Roles

### 1. `predicate.c` (Main Implementation - 5053 lines)

**Core Functions** (with line numbers):

| Function | Lines | Purpose |
|----------|-------|---------|
| `GetSerializableTransactionSnapshot()` | 1672-1698 | Entry point: get/create snapshot |
| `GetSerializableTransactionSnapshotInt()` | 1558-1670 | Internal: core snapshot logic |
| `PreCommit_CheckForSerializationFailure()` | 1778-1850 | Commit-time validation |
| `OnConflict_CheckForSerializationFailure()` | 485-700 | Dangerous structure detection |
| `CheckForSerializableConflictOut()` | 1221-1350 | Conflict detection on reads |
| `CheckForSerializableConflictIn()` | 1290-1420 | Conflict detection on writes |
| `PredicateLockRelation()` | 1850-1880 | Acquire relation-level lock |
| `PredicateLockPage()` | 1880-1920 | Acquire page-level lock |
| `PredicateLockTuple()` | 1920-1980 | Acquire tuple-level lock |
| `PredicateLockAcquire()` | 1400-1600 | Core lock acquisition |
| `ReleasePredicateLocks()` | 2100-2200 | Cleanup on commit/abort |
| `SummarizeOldestCommittedSxact()` | 2300-2450 | SLRU summarization |

**Helper Functions**:
- `InitPredicateLocks()` - Server startup initialization
- `PredicateLockHashCodeFromTargetHashCode()` - Hash computation
- `MaintainPredicateLocks()` - Maintenance routines
- `CheckPromotePredicateLock()` - Promotion decision
- `RollbackPredicateLocks()` - Rollback cleanup
- Various list/hash management functions

**Shared Memory** (allocated here):
- `SerialControlData` - Global control block
- `SERIALIZABLEXACT` hash table - Active transactions
- `PREDICATELOCKTAG` hash table - Lock targets
- `PREDICATELOCK` array - Individual locks
- Conflict edge pool - RWConflict entries

**Lock Management**:
- `SerializableXactHashLock` - Protects transaction hash
- `PredicateLockHashLock` - Protects lock hash
- `SerializableFinishedListLock` - Protects finished list
- Partition locks (128) - Per-bucket scalability

### 2. `predicate.h` (Public API - 52 lines)

**Exported Functions**:
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

### 3. `predicate_internals.h` (Internal Structures - 400 lines)

**Data Structures**:
```c
typedef struct SERIALIZABLEXACT { ... }  // ~200 bytes
typedef struct PREDICATELOCK { ... }     // ~40 bytes
typedef struct PREDICATELOCKTAG { ... }  // ~32 bytes
typedef struct PREDICATELOCKTARGET { ... } // ~32 bytes
typedef struct RWConflictData { ... }    // ~32 bytes
typedef struct SerialControlData { ... } // ~100 bytes
typedef struct LOCALPREDICATELOCK { ... } // ~32 bytes
typedef struct PredXactListData { ... }   // ~64 bytes
```

### 4. `xact.c` (Transaction Manager - Integration Points)

**Integration with SSI**:
- Line ~1500: `GetTransactionSnapshot()` calls `GetSerializableTransactionSnapshot()`
- Line ~2000: `CommitTransaction()` calls `PreCommit_CheckForSerializationFailure()`
- Line ~2200: `AbortTransaction()` calls `ReleasePredicateLocks()`

### 5. `snapmgr.c` (Snapshot Manager - Integration Points)

**Integration with SSI**:
- Calls `GetSerializableTransactionSnapshot()` for SERIALIZABLE isolation
- Returns snapshot with xmin/xmax/xip[] arrays
- Used for both MVCC visibility and SSI conflict tracking

### 6. `README-SSI` (Conceptual Documentation)

**Location**: `./src/backend/storage/lmgr/README-SSI`  
**Length**: ~900 lines of pseudocode and algorithm explanation

**Sections**:
1. Overview: What is SSI, why not 2PL
2. Snapshot semantics and MVCC integration
3. Predicate lock granularities and promotion
4. Conflict graph and dangerous structures
5. Read-only optimization and safe snapshots
6. Implementation details and data structures
7. Recovery and crash handling
8. Known limitations and future work

---

## Module Organization

### Predicate Lock Module
```
predicate.c
├── Initialization: InitPredicateLocks()
├── Snapshot Management: GetSerializableTransactionSnapshot*()
├── Lock Acquisition: PredicateLock*(), PredicateLockAcquire()
├── Conflict Detection: CheckForSerializableConflict*()
├── Commit Validation: PreCommit_CheckForSerializationFailure()
├── Cleanup: ReleasePredicateLocks()
└── Maintenance: SummarizeOldestCommittedSxact()
```

### External Integration Points
```
heapam.c → CheckForSerializableConflictOut()  (on tuple read)
         → CheckForSerializableConflictIn()   (on tuple write)

nbtree.c → PredicateLockPage()                (index scan)
         → PredicateLockTuple()

xact.c  → GetSerializableTransactionSnapshot() (BEGIN)
        → PreCommit_CheckForSerializationFailure() (COMMIT)
        → ReleasePredicateLocks()             (ABORT/COMMIT)

snapmgr.c → GetSerializableTransactionSnapshot() (snapshot request)
```

---

## Call Hierarchy (Main Paths)

### Path 1: Transaction Begin
```
xact.c:BeginTransaction()
├─ SetTransactionIsolationLevel(SERIALIZABLE)
└─ GetTransactionSnapshot()
   └─ snapmgr.c:GetTransactionSnapshot()
      └─ predicate.c:GetSerializableTransactionSnapshot()
         └─ GetSerializableTransactionSnapshotInt()
            ├─ Lock(SerializableXactHashLock)
            ├─ Allocate SERIALIZABLEXACT
            ├─ Initialize snapshot
            └─ Unlock(SerializableXactHashLock)
```

### Path 2: Data Read (with conflict detection)
```
executor:ExecSeqScan()
├─ heapam.c:heap_getnext()
└─ Loop over tuples:
   ├─ Check visibility (MVCC)
   └─ predicate.c:CheckForSerializableConflictOut()
      ├─ PredicateLockRelation() or PredicateLockPage()
      ├─ Find conflicts with prior writes
      └─ Create RWConflict edges if found
```

### Path 3: Data Write (with conflict detection)
```
executor:ExecInsert/Update/Delete()
├─ heapam.c:heap_insert/update/delete()
├─ predicate.c:CheckForSerializableConflictIn()
│  ├─ Find prior read predicates
│  ├─ Create RWConflict edges
│  └─ Check for dangerous structures
└─ Continue or abort based on result
```

### Path 4: Commit Validation
```
xact.c:CommitTransaction()
├─ predicate.c:PreCommit_CheckForSerializationFailure()
│  ├─ Lock(SerializableXactHashLock)
│  ├─ If RO_SAFE: return OK
│  ├─ Else: OnConflict_CheckForSerializationFailure()
│  │  ├─ Full dangerous structure scan
│  │  └─ Decide: abort or commit
│  └─ Unlock(SerializableXactHashLock)
├─ If conflict found: raise SERIALIZATION_FAILURE
└─ Else: proceed with actual commit
```

### Path 5: Transaction Cleanup
```
xact.c:AbortTransaction() or CommitTransaction()
├─ predicate.c:ReleasePredicateLocks()
│  ├─ Lock(SerializableXactHashLock)
│  ├─ Iterate transaction's locks
│  ├─ Unlink from hash tables
│  ├─ Free SERIALIZABLEXACT
│  └─ Unlock(SerializableXactHashLock)
└─ Continue cleanup
```

---

## Shared Memory Layout

```
Shared Memory (allocated at server startup)
│
├─ SerialControlData (1 block, ~100 bytes)
│  ├─ OldestCommittedXmin
│  ├─ SummarizeOldestCommittedSxact_offset (in SLRU)
│  ├─ Lock definitions
│  └─ Allocation pointers
│
├─ SERIALIZABLEXACT hash table
│  ├─ Hash function: VirtualTransactionId
│  ├─ Entries: ~100-1000 during normal operation
│  └─ Size: num_entries × 200 bytes
│
├─ PREDICATELOCKTAG hash table
│  ├─ Hash function: relation + page/tid + hash
│  ├─ Entries: up to max_predicate_locks
│  └─ Size: max_predicate_locks × 40 bytes
│
├─ PREDICATELOCK array
│  ├─ Max entries: max_predicate_locks (default 262144)
│  ├─ Each entry: ~40 bytes
│  └─ Total: ~10 MB (default)
│
└─ Conflict pool
   ├─ RWConflict edges in memory pool
   ├─ Linked list allocation
   └─ Size: bounded by available SHMEM
```

---

## Configuration Parameters

| Parameter | File | Line | Type | Default |
|-----------|------|------|------|---------|
| `max_predicate_locks` | `guc.c` | ~2500 | int | 262144 |
| `max_predicate_locks_per_transaction` | `guc.c` | ~2510 | int | 64 |
| `max_predicate_locks_per_relation` | `guc.c` | ~2520 | int | -1 (unlimited) |

---

## Cross-Module Dependencies

```
predicate.c depends on:
├─ predicate.h (own public API)
├─ predicate_internals.h (data structures)
├─ lock.h (LWLock infrastructure)
├─ shmem.h (shared memory)
├─ procarray.h (backend status)
├─ slru.h (SLRU for old transactions)
└─ xact.h (transaction state)

External modules depend on predicate.h:
├─ xact.c (transaction coordination)
├─ snapmgr.c (snapshot management)
├─ heapam.c (heap access method)
├─ nbtree.c (B-tree index)
└─ executor/* (query executor)
```

---

## Testing Infrastructure

**Files related to SSI testing**:
- `src/test/isolation/` - Serialization conflict test cases
- `src/test/isolation/specs/` - Test specifications in SQL
- `src/test/regress/` - Standard regression tests
- `src/test/modules/test_predicate_lock/` - Unit tests for predicate locks

**Sample isolation test**:
```
# File: src/test/isolation/specs/dangerous-1.spec
# Tests dangerous structure detection

session s1
  step s1_begin { BEGIN ISOLATION LEVEL SERIALIZABLE; }
  step s1_read  { SELECT * FROM t WHERE id = 1; }
  ...
```

---

## See Also

- [Symbol Index](appendix_symbol_index.md) - Function and struct reference
- [Glossary](appendix_glossary.md) - Term definitions
- README-SSI - In-source conceptual documentation
- PostgreSQL official documentation on SSI
