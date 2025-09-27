# GetRunningTransactionData

## Location
[src/backend/storage/ipc/procarray.c:2693-2878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2693-L2878)

## Overview
GetRunningTransactionData returns comprehensive information about all currently running transactions, including both main transactions and subtransactions, primarily used for checkpointing and standby server coordination.

## Definition

```c
RunningTransactions
GetRunningTransactionData(void)
```
## Detailed Description
This function collects and returns detailed information about all running transactions in the system. Unlike GetSnapshotData which is optimized for snapshot creation, GetRunningTransactionData provides more comprehensive information including VACUUM processes and prepared transactions. It is specifically designed for checkpointing operations and standby server coordination.

The function acquires both XidGenLock and ProcArrayLock to ensure consistency during data collection. The caller is responsible for releasing these locks after WAL-logging the snapshot information. This locking strategy prevents new XIDs from entering the proc array and transactions from committing until the snapshot is safely recorded.

The function allocates memory statically and returns a pointer to this static structure, making it non-reentrant. It collects both main transaction IDs and subtransaction IDs, handling cases where subtransaction caches have overflowed.

Key behaviors include:
- Collects all transactions with valid TransactionIDs
- Tracks oldest running transaction globally and per-database
- Handles subtransaction overflow scenarios
- Includes prepared transactions (dummy PGPROCs)
- Never executed during recovery (no KnownAssignedXids handling needed)

## Parameters / Member Variables
This function takes no parameters but returns a RunningTransactions structure containing:
- : Count of main transactions
- : Count of subtransactions  
- : Status indicating if subtransactions are in array or subtrans
- : Next transaction ID to be assigned
- : Oldest transaction ID still running system-wide
- : Oldest transaction ID running in current database
- : Most recent completed transaction ID
- : Array containing all collected transaction IDs

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)  
  - XidFromFullTransactionId
  - TransactionIdIsValid
  - TransactionIdIsNormal
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - UINT32_ACCESS_ONCE
  - pg_read_barrier
  - malloc
- Called from (representative examples):
  - [LogStandbySnapshot](../L/LogStandbySnapshot.md) (src/backend/storage/ipc/standby.c:1306)

## Notes and Other Information
- Only executed during normal operation, never during recovery
- Caller must release XidGenLock and ProcArrayLock after use
- Returns statically allocated data structure - not thread safe
- Memory for transaction ID array is allocated once and reused
- Designed primarily for background writer process during checkpoints
- Handles subtransaction overflow by setting appropriate status flags
- Does not update snapshot counters, leaving that to GetSnapshotData
- Includes duplicate TransactionIds from prepared transactions finishing preparation

## Simplified Source

```c
// Simplified version of GetRunningTransactionData
RunningTransactions GetRunningTransactionData(void) {
    // Static result workspace (reused across calls)
    static RunningTransactionsData CurrentRunningXactsData;

    ProcArrayStruct *arrayP = procArray;
    TransactionId *other_xids = ProcGlobal->xids;
    RunningTransactions result = &CurrentRunningXactsData;
    TransactionId *xids;
    int count = 0, subcount = 0;
    bool suboverflowed = false;

    // Allocate memory for transaction IDs on first call
    if (result->xids == NULL) {
        result->xids = malloc(TOTAL_MAX_CACHED_SUBXIDS * sizeof(TransactionId));
        // Handle allocation failure
    }
    xids = result->xids;

    // Acquire locks to ensure consistent snapshot
    LWLockAcquire(ProcArrayLock, LW_SHARED);
    LWLockAcquire(XidGenLock, LW_SHARED);

    // Initialize tracking variables from global transaction state
    TransactionId latestCompletedXid = XidFromFullTransactionId(TransamVariables->latestCompletedXid);
    TransactionId oldestRunningXid = XidFromFullTransactionId(TransamVariables->nextXid);
    TransactionId oldestDatabaseRunningXid = oldestRunningXid;

    // First pass: collect all main transaction IDs
    for (int index = 0; index < arrayP->numProcs; index++) {
        PGPROC *proc = &allProcs[arrayP->pgprocnos[index]];
        TransactionId xid = UINT32_ACCESS_ONCE(other_xids[index]);

        // Skip processes without valid transaction IDs
        if (!TransactionIdIsValid(xid))
            continue;

        // Track oldest running transactions
        if (TransactionIdPrecedes(xid, oldestRunningXid))
            oldestRunningXid = xid;

        if (proc->databaseId == MyDatabaseId &&
            TransactionIdPrecedes(xid, oldestDatabaseRunningXid))
            oldestDatabaseRunningXid = xid;

        // Check for subtransaction overflow
        if (ProcGlobal->subxidStates[index].overflowed)
            suboverflowed = true;

        xids[count++] = xid;
    }

    // Second pass: collect subtransaction IDs if no overflow occurred
    if (!suboverflowed) {
        for (int index = 0; index < arrayP->numProcs; index++) {
            PGPROC *proc = &allProcs[arrayP->pgprocnos[index]];
            int nsubxids = ProcGlobal->subxidStates[index].count;

            if (nsubxids > 0) {
                // Copy subtransaction IDs to result array
                memcpy(&xids[count], proc->subxids.xids,
                       nsubxids * sizeof(TransactionId));
                count += nsubxids;
                subcount += nsubxids;
            }
        }
    }

    // Fill in the result structure
    result->xcnt = count - subcount;  // Main transaction count
    result->subxcnt = subcount;       // Subtransaction count
    result->subxid_status = suboverflowed ? SUBXIDS_IN_SUBTRANS : SUBXIDS_IN_ARRAY;
    result->nextXid = XidFromFullTransactionId(TransamVariables->nextXid);
    result->oldestRunningXid = oldestRunningXid;
    result->oldestDatabaseRunningXid = oldestDatabaseRunningXid;
    result->latestCompletedXid = latestCompletedXid;

    // Caller must release locks after WAL-logging the snapshot
    return result;
}
```

Key simplifications made:
- Removed detailed error handling and memory allocation checks for clarity
- Consolidated variable declarations at the top
- Simplified loop logic while preserving the two-pass structure
- Abstracted memory barrier and locking details
- Focused on the core algorithm: collect main XIDs, then subtransaction XIDs
- Removed verbose comments and kept only essential logic explanations
- Maintained the essential structure of collecting running transaction data for checkpointing