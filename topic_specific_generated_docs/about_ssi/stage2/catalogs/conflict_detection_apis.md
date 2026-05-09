# SSI Conflict Detection APIs Catalog

## Core Functions

### 1. CheckForSerializableConflictOut()

**Source**: `./src/backend/storage/lmgr/predicate.c:1435`  
**Importance**: 0.95

**Signature**:
```c
void CheckForSerializableConflictOut(
    Relation relation,
    TransactionId xid,
    Snapshot snapshot)
```

**Purpose**: Detect when current transaction reads data written by concurrent transaction

**When Called**:
- Heap/Index visibility checks when tuple found visible
- `xid` represents transaction that wrote tuple
- `snapshot->xmin` > tuple's writer XID (concurrent write)

**Algorithm**:
1. Check if `xid` in progress (in snapshot's xip list)
2. Look up writer's SERIALIZABLEXACT via SerializableXidHash
3. If found and not read-only: call `CheckForSerializableConflictOut()` inner
4. Create rw-conflict edge: currentXact (reader) → writerXact
5. Trigger dangerous structure detection

**Error Handling**: None - informational only

**Optimization**: Skips if current transaction is read-only and safe

**Called From**:
- heapam.c: heap_fetch(), HeapTupleSatisfiesMVCC()
- nbtree.c: Index scan visibility
- Multiple other AM implementations

### 2. CheckForSerializableConflictIn()

**Source**: `./src/backend/storage/lmgr/predicate.c:1512`  
**Importance**: 0.95

**Signature**:
```c
void CheckForSerializableConflictIn(
    Relation relation,
    ItemPointer tid,
    BlockNumber blkno)
```

**Purpose**: Detect when current transaction writes to data that concurrent transactions have read

**When Called**:
- Before INSERT (tid from new tuple)
- Before UPDATE (tid of updated tuple)
- Before DELETE (tid of deleted tuple)

**Algorithm**:
1. Check if relation participates in predicate locking
2. Scan three lock granularities: Relation, Page, Tuple
3. For each lock found: call CheckForSerializableConflictIn()
4. Create rw-conflict: lockholder (reader) → currentXact (writer)
5. Trigger dangerous structure detection

**Lock Target Scan Order**:
```
First:  Check PredicateLockTargetHash for RELATION lock
Second: Check PredicateLockTargetHash for PAGE lock
Third:  Check PredicateLockTargetHash for TUPLE lock
```

**Error Handling**: None - informational

**Performance**: O(n) where n = number of active transactions with locks on target

**Called From**:
- heap_insert() - before heap insert
- heap_update() - before tuple update
- heap_delete() - before tuple delete
- heap_multi_insert() - for bulk inserts

---

## Conflict Edge Creation

### 3. OnConflict_CheckForSerializationFailure()

**Source**: `./src/backend/storage/lmgr/predicate.c:1315`  
**Importance**: 0.98 (THE CORE ALGORITHM)

**Signature**:
```c
void OnConflict_CheckForSerializationFailure(void)
```

**Purpose**: Core SSI conflict detection - search for dangerous structure pattern

**Algorithm**:

```
For each SERIALIZABLEXACT with:
  - Has inConflict (reader of others)
  - Has outConflict (writer to others)
  
  Dangerous Structure Check:
    For each inConflict edge (Tout → this):
      If Tout COMMITTED:
        For each outConflict edge (this → Tpivot):
          Search from Tpivot for path back to Tout
          If path exists:
            DANGEROUS STRUCTURE FOUND
            Mark victim as DOOMED
            Break
```

**Time Complexity**: O(d) where d = conflict graph depth

**Optimization**: Uses "summarization" to reduce search space (Cahill optimization)

**Called From**:
- CheckForSerializableConflictOut() - after creating edge
- CheckForSerializableConflictIn() - after creating edge
- PreCommit_CheckForSerializationFailure() - final check

**Safety Guarantees**: Never false-negative (always finds real anomalies)

---

## Support Functions

### 4. FlagRWConflict()

**Source**: `./src/backend/storage/lmgr/predicate.c:2250`  
**Importance**: 0.90

**Signature**:
```c
void FlagRWConflict(
    SERIALIZABLEXACT *reader,
    SERIALIZABLEXACT *writer,
    bool write_after_read)
```

**Purpose**: Create rw-conflict edge between two transactions

**Parameters**:
- `reader`: Transaction that reads
- `writer`: Transaction that writes
- `write_after_read`: TRUE if write happens after read

**Behavior**:
- Allocates RWConflictData from pool
- Adds to reader's inConflicts list
- Adds to writer's outConflicts list
- If pools empty: promotes locks to free memory

**Idempotent**: Safe to call multiple times for same pair (only one edge)

---

### 5. RWConflictExists()

**Source**: `./src/backend/storage/lmgr/predicate.c:2300`  
**Importance**: 0.80

**Signature**:
```c
bool RWConflictExists(
    const SERIALIZABLEXACT *reader,
    const SERIALIZABLEXACT *writer)
```

**Purpose**: Check if rw-conflict edge already exists

**Returns**: TRUE if edge found, FALSE otherwise

**Uses**: Prevent duplicate edge creation

---

### 6. SetPossibleUnsafeConflict()

**Source**: `./src/backend/storage/lmgr/predicate.c:2330`  
**Importance**: 0.80

**Purpose**: Record possible conflict for read-only transaction safety checking

**Used By**: Read-only safe snapshot detection

---

## Conflict Detection Patterns

### Pattern 1: Sequential Read + Write

```c
// T1: Sequential scan
PredicateLockRelation(users_table, snapshot);
// T1 reads entire table

// T2: Concurrent write
UPDATE users SET status = 'active' WHERE id = 5;
// This calls CheckForSerializableConflictIn()
// Finds T1's relation lock
// Creates conflict: T1 (reader) ← T2 (writer)
```

### Pattern 2: Concurrent Read + Write

```c
// T1: Reads specific row
SELECT * FROM users WHERE id = 5;
// Calls PredicateLockTID() for row id=5

// T2: Deletes same row
DELETE FROM users WHERE id = 5;
// This calls CheckForSerializableConflictIn()
// Finds T1's tuple lock
// Creates conflict: T1 (reader) ← T2 (writer)
```

### Pattern 3: Phantom Read

```c
// T1: Read users >= 100
SELECT * FROM users WHERE id >= 100;
// Predicate locks on: range represented by pages

// T2: Insert user with id = 150
INSERT INTO users VALUES (150, ...);
// Finds T1's page lock
// Creates conflict: T1 (reader) ← T2 (writer)
```

---

## Conflict Detection Complexity Analysis

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| CheckForSerializableConflictOut | O(1) | Fast hash lookup |
| CheckForSerializableConflictIn | O(n) | Scan all locks on target |
| OnConflict_CheckForSerializationFailure | O(d) | Depth of conflict graph |
| FlagRWConflict | O(m) | m = pool size (rare) |
| RWConflictExists | O(1) | Fast list search |

---

## Integration with Query Execution

### Read Path

```
1. ExecutorStart()
   └─ GetSerializableTransactionSnapshot()

2. ExecutorRun() - Per-tuple
   └─ HeapNext()
      └─ HeapFetchTuple()
         ├─ Check visibility (xmin/xmax)
         └─ CheckForSerializableConflictOut()
            └─ OnConflict_CheckForSerializationFailure()

3. ProcessQueryDesc()
   └─ Check for DOOMED flag
```

### Write Path

```
1. heap_insert()
   └─ CheckForSerializableConflictIn()
      └─ OnConflict_CheckForSerializationFailure()

2. heap_update()
   └─ CheckForSerializableConflictIn()
      └─ OnConflict_CheckForSerializationFailure()

3. heap_delete()
   └─ CheckForSerializableConflictIn()
      └─ OnConflict_CheckForSerializationFailure()
```

---

## Conflict Detection Optimizations

### Optimization 1: Lock Coalescing

When memory pressure high:
- Fine-grained (tuple) locks → coarser (page/relation) locks
- Fewer checks needed in CheckForSerializableConflictIn()
- Fewer edges in conflict graph

### Optimization 2: Summarization

For old transactions:
- Combine conflicts into "OldCommittedSxact" node
- Reduces graph size
- Maintains safety guarantees

### Optimization 3: Quick Path

Read-only transactions with safe snapshot:
- Skip conflict detection entirely
- No predicate locks acquired
- Major performance win for RO workloads

