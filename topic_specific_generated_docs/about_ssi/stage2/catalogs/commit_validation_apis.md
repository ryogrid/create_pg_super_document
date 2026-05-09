# SSI Commit Validation and Abort APIs Catalog

## Commit-Time Validation

### 1. PreCommit_CheckForSerializationFailure()

**Source**: `./src/backend/storage/lmgr/predicate.c:1315`  
**Importance**: 0.95 (CRITICAL PATH)

**Signature**:
```c
void PreCommit_CheckForSerializationFailure(void)
```

**When Called**:
- CommitTransaction() in xact.c before actual commit
- Called ONCE per transaction at commit time
- After all query execution complete

**Purpose**: Final validation before commit

**Algorithm**:

```
1. Lock SerializableFinishedListLock

2. If transaction already marked DOOMED:
   raise ERROR serialization_failure
   return

3. If not read-only:
   Set prepareSeqNo = LastSxactCommitSeqNo++

4. For each possible Tpivot (has edge from our Tin):
   For each possible Tout (has edge to our reader):
      If dangerous structure Tin → Tpivot → Tout:
         Mark THIS transaction DOOMED
         Raise ERROR serialization_failure
         return

5. Mark as COMMITTED
6. Record in SLRU commit history
7. Unlock
```

**Invariant**: Never marks transaction DOOMED after it committed

**Error Behavior**:
- Raises SQLSTATE 40001 (SERIALIZATION_FAILURE)
- Triggers full transaction rollback
- Application must retry

**Transaction State**:
```
Before:  ACTIVE (executing)
After:   COMMITTED or (DOOMED → ERROR)
```

**Performance**:
- O(d) where d = conflict graph depth
- Average 0.1-1ms for most transactions
- Can be 10-100ms for high-contention workloads

---

## Transaction Release Functions

### 2. ReleasePredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:1224`  
**Importance**: 0.90

**Signature**:
```c
void ReleasePredicateLocks(
    bool isCommit,
    bool isReadOnlySafe)
```

**When Called**:
- At transaction end: COMMIT or ABORT
- For read-only safe: before actual commit

**Parameters**:
- `isCommit`: TRUE for commit, FALSE for abort
- `isReadOnlySafe`: TRUE if RO-safe determined

**Behavior**:

| isCommit | isReadOnlySafe | Action |
|----------|---|---------|
| TRUE | FALSE | Keep locks (overlap tracking) |
| TRUE | TRUE | Release locks immediately |
| FALSE | - | Release locks immediately |

**Effect**:
1. Update SERIALIZABLEXACT record
2. Move to FinishedSerializableTransactions list (if commit)
3. Remove from active list
4. Clear local backend state

---

### 3. ReleaseOneSerializableXact()

**Source**: `./src/backend/storage/lmgr/predicate.c:2200`  
**Importance**: 0.88

**Signature**:
```c
void ReleaseOneSerializableXact(
    SERIALIZABLEXACT *sxact,
    bool partial,
    bool isCommit)
```

**Purpose**: Release single transaction from finished list

**When Called**:
- ClearOldPredicateLocks() periodic cleanup
- During crash recovery
- Manual cleanup

**Parameters**:
- `sxact`: Transaction to release
- `partial`: TRUE if partial release (parallel workers)
- `isCommit`: TRUE if committed, FALSE if aborted

**Behavior**:
1. Check if still overlaps with active transactions
2. If no overlap: remove all predicate locks
3. Clear conflict edges
4. Return to pool
5. Return conflict edges to pool

**Safety**: Only removes if safe (no active overlap)

---

### 4. ClearOldPredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:1273`  
**Importance**: 0.82

**Signature**:
```c
void ClearOldPredicateLocks(void)
```

**Purpose**: Periodic garbage collection of released transactions

**When Called**:
- Autovacuum process
- Called periodically (vacuum schedule)
- After significant transaction load

**Algorithm**:
1. Lock SerializableFinishedListLock
2. Walk FinishedSerializableTransactions list
3. For each finished transaction:
   - Check if overlaps with any active
   - If no overlap: ReleaseOneSerializableXact()
4. Update CanPartialClearThrough marker

**Impact**: Reclaims memory from released transactions

---

## Transaction State Transitions

### Abort Path Functions

### 5. MarkSxactDoomed()

**Source**: `./src/backend/storage/lmgr/predicate.c:1380`  
**Importance**: 0.85

**Signature**:
```c
void MarkSxactDoomed(SERIALIZABLEXACT *sxact)
```

**Purpose**: Mark transaction for abort

**Behavior**:
- Sets SXACT_FLAG_DOOMED
- Logged but doesn't abort immediately
- Aborts at next executor check or at commit

**Called From**:
- OnConflict_CheckForSerializationFailure() - dangerous structure found
- PreCommit_CheckForSerializationFailure() - final check

---

## 2PC Support Functions

### 6. AtPrepare_PredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:2540`  
**Importance**: 0.85

**Signature**:
```c
void AtPrepare_PredicateLocks(void)
```

**When Called**: During PREPARE phase of two-phase commit

**Behavior**:
1. Serialize all predicate locks to state file
2. Create TwoPhasePredicateLockRecord for each lock
3. Write to WAL
4. Keep in-memory copy until COMMIT/ROLLBACK

**Safety**: Ensures locks survive server crash

---

### 7. PostPrepare_PredicateLocks()

**Source**: `./src/backend/storage/lmgr/predicate.c:2570`  
**Importance**: 0.80

**When Called**: After XID assigned to prepared transaction

**Behavior**:
1. Update SERIALIZABLEXACT with permanent XID
2. Mark as PREPARED (not yet COMMITTED)
3. Keep locks until COMMIT PREPARED

---

### 8. PredicateLockTwoPhaseFinish()

**Source**: `./src/backend/storage/lmgr/predicate.c:2600`  
**Importance**: 0.82

**Signature**:
```c
void PredicateLockTwoPhaseFinish(
    TransactionId xid,
    bool isCommit)
```

**When Called**: At COMMIT PREPARED or ROLLBACK PREPARED

**Behavior**:

| isCommit | Action |
|----------|--------|
| TRUE | Move to FinishedList, keep locks for overlap |
| FALSE | Immediate cleanup |

---

### 9. predicatelock_twophase_recover()

**Source**: `./src/backend/storage/lmgr/predicate.c:2630`  
**Importance**: 0.85

**Signature**:
```c
void predicatelock_twophase_recover(
    TransactionId xid,
    uint16 info,
    void *recdata,
    uint32 len)
```

**When Called**: During crash recovery, processing 2PC WAL records

**Purpose**: Reconstruct predicate locks from WAL

**Algorithm**:
1. Deserialize lock records from WAL
2. Recreate PredicateLockTargets and PredicateLocks
3. Restore SERIALIZABLEXACT records
4. Continue conflict detection as if prepared

**Safety**: Ensures no lock loss during crash

---

## SLRU Commit History

### 10. SerialAdd()

**Source**: `./src/backend/storage/lmgr/predicate.c:2700`

**Purpose**: Record transaction in SLRU commit history

**When Called**: At transaction commit (after validation)

**Behavior**:
- Stores commitSeqNo in SLRU
- Allows crash recovery
- Tracks conflict history

---

## Error Handling

### Serialization Failure Error

```c
// Raised by PreCommit_CheckForSerializationFailure()
ereport(ERROR,
    (errcode(ERRCODE_SERIALIZATION_FAILURE),
     errmsg("could not serialize access due to concurrent update")));
```

**SQLSTATE**: 40001 (SERIALIZATION_FAILURE)

**Client Receives**:
```
ERROR: could not serialize access due to concurrent update
```

**Transaction State**: Fully rolled back, all changes discarded

---

## Performance Characteristics

| Function | Time | Contention |
|----------|------|-----------|
| PreCommit_CheckForSerializationFailure | O(d) | High at peak |
| ReleasePredicateLocks | O(k) | Medium |
| ReleaseOneSerializableXact | O(m) | Low |
| ClearOldPredicateLocks | O(n) | Low (background) |
| MarkSxactDoomed | O(1) | Low |
| AtPrepare_PredicateLocks | O(k) | Low |
| PredicateLockTwoPhaseFinish | O(k) | Low |

Where:
- d = conflict graph depth
- k = locks held by transaction
- m = conflicts involving transaction
- n = finished transactions

---

## Integration with Transaction Manager

### Commit Path

```
CommitTransaction()
  ├─ PreCommit_CheckForSerializationFailure()
  │  └─ May raise ERROR 40001
  ├─ RecordTransactionCommit()
  ├─ ReleasePredicateLocks(true, false)
  └─ Clear transaction state
```

### Abort Path

```
AbortTransaction()
  ├─ ReleasePredicateLocks(false, false)
  └─ Clear all state immediately
```

### 2PC Prepare Path

```
CommitTransactionCommand() [PREPARE]
  ├─ AtPrepare_PredicateLocks()
  ├─ PostPrepare_PredicateLocks()
  └─ State written to state file
```

### 2PC Finish Path

```
FinishPreparedTransaction()
  ├─ PreCommit_CheckForSerializationFailure() [for COMMIT PREPARED]
  ├─ PredicateLockTwoPhaseFinish(xid, isCommit)
  └─ Clear resources
```

