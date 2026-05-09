# PostgreSQL SSI: Commit Validation and Abort Paths

## Overview

At transaction commit, SSI performs final validation to detect serialization failures that may have been missed during execution. If validation fails, the transaction is aborted with `SERIALIZATION_FAILURE`. This component describes the commit-time checks and the abort paths.

**Key Insight**: SSI commits may fail due to dangerous structures detected at commit time, not just during execution. This is why retry logic is essential for SSI applications.

## Commit-Time Validation Pipeline

```
CommitTransaction()
├── xact.c: PreCommit_CheckForSerializationFailure()
│   ├── Mark transaction as PREPARED
│   ├── Increment prepareSeqNo
│   ├── Search for dangerous structures
│   ├── If found:
│   │   ├── Mark transaction as DOOMED
│   │   └── ereport(SERIALIZATION_FAILURE)
│   └── Proceed with commit
├── xact.c: RecordTransactionCommit()
│   ├── Record commit in WAL
│   ├── Update commitSeqNo
│   └── Mark as COMMITTED
└── xact.c: AtCommit_PredicateLocks()
    ├── ReleasePredicateLocks(true, false)
    └── Record predicate locks for SLRU
```

## Core Validation Functions

### 1. PreCommit_CheckForSerializationFailure() - Main Validation

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.95 (critical path, entry point)

#### Signature
```c
void PreCommit_CheckForSerializationFailure(void)
```

#### Purpose
Final serialization check before committing. Searches for dangerous structures that weren't detected during execution. If found and cycle completed, aborts transaction.

#### Algorithm Overview

```
function PreCommit_CheckForSerializationFailure():
    
    if MySerializableXact is NULL:
        return  // Not a serializable transaction
    
    if SxactIsReadOnly(MySerializableXact):
        // Read-only transactions can't create conflicts
        // They can only be victims of dangerous structures
        // Decision: don't abort read-only at commit
        return
    
    // Now: read-write transaction at commit time
    // Has it been marked DOOMED by dangerous structures?
    
    if SxactIsDoomed(MySerializableXact):
        // Already detected as part of cycle
        ereport(SERIALIZATION_FAILURE)
        return
    
    // Additional check: could commit cause a cycle?
    // Final scan for dangerous structures with us as Tout
    
    LWLockAcquire(SerializableFinishedListLock, LW_EXCLUSIVE)
    
    // Check all incoming conflicts
    for each conflict in MySerializableXact->inConflicts:
        Tin = conflict->sxactOut
        
        // Pattern: Tin -> Me (Tpivot becomes implicit)
        // Look for: Tin has incoming conflict (Tin -> someone -> Me)
        
        if DangerousStructureExists(Tin):
            MarkSxactDoomed(MySerializableXact)
            LWLockRelease(...)
            ereport(SERIALIZATION_FAILURE)
            return
    
    LWLockRelease(SerializableFinishedListLock)
    
    // No serialization failure detected
    // Proceed with commit...
```

#### Dangerous Structure Search at Commit

The final validation scans for:

```
    Tin ─rw→ (intermediate transactions) ─rw→ Me (Tpivot/Tout)

Detection: If Tin has edge into MY inConflicts,
          AND I'm about to commit (Tout role),
          AND there exists T such that Tin → T → Me (rw-conflicts),
          THEN dangerous structure detected
```

#### Read-Only Optimization at Commit

For read-only transactions:

```c
if SxactIsReadOnly(MySerializableXact):
    // Check if snapshot was safe
    // Set SXACT_FLAG_RO_SAFE if no conflicts found
    
    if can_determine_safe_snapshot():
        SetSafeSnapshot()
    
    // Read-only commits never cause rollback
    return
```

#### Marked as DOOMED

Once a transaction is marked DOOMED:

```c
#define SxactIsDoomed(sxact) \
    ((sxact)->flags & SXACT_FLAG_DOOMED)

// Caller: don't do further work
// Executor: arrange rollback
// Other transactions: ignore this one
```

#### Called From

Called from `xact.c:CommitTransaction()`:

```c
CommitTransaction() {
    // ... pre-commit setup ...
    
    if (IsolationLevel == SERIALIZABLE) {
        PreCommit_CheckForSerializationFailure();
        // May ereport(ERROR) with SERIALIZATION_FAILURE
    }
    
    // ... proceed with commit ...
}
```

---

### 2. ReleaseOneSerializableXact() - Transaction Cleanup

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.88 (cleanup critical path)

#### Signature
```c
static void ReleaseOneSerializableXact(
    SERIALIZABLEXACT *sxact,
    bool partial,
    bool summarize)
```

#### Parameters
- `sxact` (SERIALIZABLEXACT*): Transaction to release
- `partial` (bool): If true, preserve for parallel workers
- `summarize` (bool): If true, write to SLRU before releasing

#### Purpose
Cleans up a transaction's predicate locks and state after it's no longer needed. Called when transaction is old enough that no concurrent transactions overlap.

#### When Called

1. **After commit**: When all overlapping transactions finish
2. **After abort**: Immediately (aborted transactions don't wait)
3. **During GC sweep**: ClearOldPredicateLocks() identifies candidates

#### Implementation

```c
void ReleaseOneSerializableXact(SERIALIZABLEXACT *sxact,
                                bool partial, bool summarize) {
    
    Assert(sxact != MySerializableXact);  // Don't release self
    
    // Step 1: If summarize, record to SLRU
    if (summarize && SxactIsCommitted(sxact)) {
        // Write commit information for conflict tracking
        // Even after predicate locks released, commit history survives
        SerialAdd(sxact->topXid, sxact->earliestOutConflictCommit);
    }
    
    // Step 2: Release predicate locks
    for each lock in sxact->predicateLocks:
        RemovePredicateLock(lock)
        RemoveTargetIfNoLongerUsed(lock->target)
    
    // Step 3: Release conflicts
    for each conflict in sxact->inConflicts:
        ReleaseRWConflict(conflict)
    
    for each conflict in sxact->outConflicts:
        ReleaseRWConflict(conflict)
    
    // Step 4: Mark or deallocate
    if partial:
        // Parallel worker case: leader will cleanup later
        sxact->flags |= SXACT_FLAG_PARTIALLY_RELEASED
    else:
        // Normal case: return to pool
        ReleasePredXact(sxact)
        RemoveSerializableXid(sxact->topXid)
}
```

#### Partial Release for Parallel Queries

```
// When parallel worker finishes but leader continues:
ReleaseOneSerializableXact(sxact, partial=true, summarize=false)

// Later, when leader finishes:
ReleaseOneSerializableXact(sxact, partial=false, summarize=true)
```

---

### 3. ClearOldPredicateLocks() - Batch Cleanup

**Source**: `./src/backend/storage/lmgr/predicate.c`  
**Importance**: 0.82 (memory pressure handling)

#### Signature
```c
static void ClearOldPredicateLocks(void)
```

#### Purpose
Periodically garbage-collects old predicate locks and transaction records. Triggered when new global xmin is set or memory pressure builds.

#### Algorithm

```
function ClearOldPredicateLocks():
    
    LWLockAcquire(SerializableFinishedListLock, LW_EXCLUSIVE)
    
    // Find transactions that are no longer needed
    SxactGlobalXmin = GetGlobalXmin()  // Min xmin of active xacts
    
    for each sxact in FinishedSerializableTransactions:
        
        // Can release if transaction finished and all overlapping xacts done
        if sxact->finishedBefore is not NULL:
            if sxact->finishedBefore < SxactGlobalXmin:
                // No active transaction overlaps this one anymore
                ReleaseOneSerializableXact(sxact, false, true)
    
    LWLockRelease(SerializableFinishedListLock)
```

#### Triggered By

1. **SetNewSxactGlobalXmin()**: After new transaction starts
2. **Timeout**: Periodic background cleanup
3. **Memory pressure**: When near limits

#### Side Effects

- Resets LocalPredicateLockHash
- Updates CanPartialClearThrough (per-transaction cleanup marker)
- May trigger SLRU page cleanup

---

## Abort Path - Forced Rollback

### MarkSxactDoomed() - Marking for Abort

**Source**: `./src/backend/storage/lmgr/predicate.c`

```c
void MarkSxactDoomed(SERIALIZABLEXACT *sxact) {
    Assert(sxact != NULL);
    
    if SxactIsDoomed(sxact):
        return  // Already marked
    
    // Set the DOOMED flag
    sxact->flags |= SXACT_FLAG_DOOMED
    
    // Signal the transaction's process if still alive
    if (sxact->pid > 0) {
        SendSignal(sxact->pid, SIGTERM)  // Actually: set interrupt flag
    }
}
```

### How Doomed Transaction Aborts

When a transaction is marked DOOMED:

```
Executor Loop:
    // Every instruction cycle check this:
    if (InterruptPending) {
        ProcessInterrupts()
        
        if (MySerializableXact && SxactIsDoomed(...)):
            ereport(ERROR, ERRCODE_SERIALIZATION_FAILURE)
            // Triggers transaction abort
    }
```

---

## Serialization Failure Handling

### Error Reporting

```c
ereport(ERROR,
    (errcode(ERRCODE_SERIALIZATION_FAILURE),
     errmsg("could not serialize access due to concurrent update"),
     errdetail("Detected dangerous structure in predicate locks")))
```

### SQL Error Code

```
SQLSTATE: 40001 ("serialization_failure")
StandardSQLState: SERIALIZATION_FAILURE
```

### Client Visibility

```sql
-- Application receives error 40001
-- Application can detect and retry automatically

BEGIN ISOLATION LEVEL SERIALIZABLE;
-- ... transactions ...
COMMIT;
-- ERROR: could not serialize access due to concurrent update
-- (SERIALIZATION_FAILURE)
```

---

## Cleanup at Abort

### ReleasePredicateLocks(false, false) - Abort Cleanup

**Source**: `./src/backend/storage/lmgr/predicate.c`

Called from `xact.c:AbortTransaction()` when transaction rolls back:

```c
void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe) {
    
    if isReadOnlySafe:
        // Read-only transaction determined safe
        // Release immediately
        Mark(RO_SAFE)
        ReleaseAllPredicateLocks()
        return
    
    if isCommit is FALSE:
        // Abort case: immediate cleanup
        Mark(ROLLED_BACK)
        ReleaseAllPredicateLocks()
        RemoveRWConflicts()
        
        // Clear flags so this sxact won't be used
        MySerializableXact = InvalidSerializableXact
        return
    
    // isCommit is TRUE: normal commit case
    // (handled differently - locks kept for overlap tracking)
    Mark(COMMITTED)
    MySerializableXact = InvalidSerializableXact
}
```

#### Aborted Transaction Implications

```c
// Once marked ROLLED_BACK:

SxactIsRolledBack(sxact):
    return (sxact->flags & SXACT_FLAG_ROLLED_BACK)

// In conflict detection:
if SxactIsRolledBack(reader):
    return  // Ignore conflicts from aborted txn

if SxactIsRolledBack(writer):
    return  // Ignore conflicts to aborted txn
```

---

## SLRU-Based Commit History

### SerialAdd() - Recording Commit

**Source**: `./src/backend/storage/lmgr/predicate.c`

Records transaction's commit information to SLRU for long-term storage:

```c
void SerialAdd(TransactionId xid, 
               SerCommitSeqNo minConflictCommitSeqNo) {
    
    // Write to SLRU page
    // Key: xid
    // Value: minConflictCommitSeqNo (smallest commit seq of conflicts)
    
    SerialValue(page, xid) = minConflictCommitSeqNo
}
```

#### Why Needed

- Predicate locks can be released after commit
- But conflict history must survive until overlapping transactions finish
- SLRU provides lightweight persistent storage
- Allows cleanup of in-memory SERIALIZABLEXACT records

### SerialGetMinConflictCommitSeqNo() - Reading History

```c
SerCommitSeqNo SerialGetMinConflictCommitSeqNo(TransactionId xid) {
    // Look up in SLRU
    // If xid committed with conflicts, return min conflict commit seq
    // If no info, return 0 (no conflicts)
}
```

---

## Integration with 2PC (Two-Phase Commit)

### At PREPARE Time

```c
AtPrepare_PredicateLocks() {
    // Write predicate locks to 2PC state file
    // Mark transaction as PREPARED (not yet committed)
}
```

### At COMMIT PREPARED Time

```c
PredicateLockTwoPhaseFinish(xid, isCommit) {
    // If isCommit:
    //   Move locks to finished list (same as normal commit)
    //   Record to SLRU
    // If not isCommit (abort):
    //   Release immediately
}
```

---

## State Transitions at Commit

```
ACTIVE ──→ PREPARED ──→ COMMITTED
          (prepareSeqNo set)
                        (commitSeqNo set)

If dangerous structure detected:
ACTIVE ──→ PREPARED ──→ DOOMED ──→ ROLLED_BACK

If aborted:
ACTIVE ──→ ROLLED_BACK
```

## Performance Characteristics

| Operation | Time | Lock Held |
|-----------|------|-----------|
| PreCommit_CheckForSerializationFailure | O(m) | SerializableFinishedListLock |
| ReleaseOneSerializableXact | O(k) | None (predicate list lock) |
| ClearOldPredicateLocks | O(n*k) | SerializableFinishedListLock |
| MarkSxactDoomed | O(1) | None |

Where:
- m = number of incoming conflicts to transaction
- k = number of predicate locks held
- n = number of finished transactions

---

## Configuration and Tuning

### GUC Parameters Affecting Commit

```c
// Commit-time dangerous structure search depth
// (internal, not user-tunable)
#define MAX_SERIALIZABLE_DEPTH 100

// Memory limits affecting cleanup
max_predicate_locks = max_connections * 64

// SLRU buffer size for commit history
serializable_buffers = 64  (pages, default)
```

### Monitoring Commit Failures

```sql
-- Track serialization failures in logs
SELECT extract(hour FROM pg_postmaster_start_time()),
       COUNT(*)
FROM pg_stat_statements
WHERE query LIKE '%SERIALIZATION_FAILURE%'
GROUP BY 1;
```

