# PrescanPreparedTransactions

## Location
[src/backend/access/transam/twophase.c:1953-2032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1953-L2032)

## Overview
PrescanPreparedTransactions scans prepared transactions in shared memory to determine the valid XID range and returns the oldest valid XID for proper pg_subtrans synchronization during database startup.

## Definition
```c
TransactionId PrescanPreparedTransactions(TransactionId **xids_p, int *nxids_p)
```

## Detailed Description
PrescanPreparedTransactions is executed during database startup after WAL reading is complete to validate and analyze prepared transactions in the TwoPhaseState shared memory. The function serves multiple critical purposes:

1. **XID Range Validation**: Validates that prepared transaction XIDs don't exceed TransamVariables->nextXid, discarding any that suggest PITR recovery to an earlier point without proper cleanup
2. **Subtransaction XID Handling**: Advances nextXid beyond any subtransaction XIDs from valid prepared transactions, since subxact commits don't generate WAL entries
3. **Minimum XID Calculation**: Determines the oldest valid XID among prepared transactions for pg_subtrans synchronization
4. **XID Collection**: Optionally collects all valid top-level XIDs into an array for the caller

The function processes each prepared transaction by calling ProcessTwoPhaseBuffer to validate the transaction state, then incorporates valid transactions into the running minimum XID calculation. Corrupted two-phase files cause immediate failure to prevent system corruption.

## Parameters / Member Variables
- `xids_p`: Optional output parameter for storing a palloc'd array of all valid top-level XIDs
- `nxids_p`: Optional output parameter for the number of XIDs in the returned array

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId
  - [ProcessTwoPhaseBuffer](ProcessTwoPhaseBuffer.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [palloc](../p/palloc.md)/repalloc/pfree
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md)
  - [xlog_redo](../x/xlog_redo.md)

## Notes and Other Information
- Returns the oldest valid XID, or TransamVariables->nextXid if no prepared transactions exist
- Discards prepared transactions with XIDs beyond nextXid to handle PITR recovery scenarios safely
- Uses exclusive lock on TwoPhaseStateLock during the scan to ensure consistency
- Dynamically allocates and grows the XID array using palloc/repalloc with doubling strategy
- Validates transaction integrity through ProcessTwoPhaseBuffer calls with specific parameters
- Critical for proper subtrans initialization and XID assignment during startup recovery

## Simplified Source

```c
// Simplified version of PrescanPreparedTransactions
TransactionId PrescanPreparedTransactions(TransactionId **xids_p, int *nxids_p) {
    FullTransactionId nextXid = TransamVariables->nextXid;
    TransactionId origNextXid = XidFromFullTransactionId(nextXid);
    TransactionId result = origNextXid;
    TransactionId *xids = NULL;
    int nxids = 0;
    int allocsize = 0;

    // Scan all prepared transactions under lock
    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
    for (int i = 0; i < TwoPhaseState->numPrepXacts; i++) {
        GlobalTransaction gxact = TwoPhaseState->prepXacts[i];
        TransactionId xid = gxact->xid;

        // Validate transaction buffer
        char *buf = ProcessTwoPhaseBuffer(xid, gxact->prepare_start_lsn,
                                         gxact->ondisk, false, true);
        if (buf == NULL)
            continue;

        // Update running minimum XID
        if (TransactionIdPrecedes(xid, result))
            result = xid;

        // Collect XID if requested
        if (xids_p) {
            // Grow array if needed
            if (nxids == allocsize) {
                if (nxids == 0) {
                    allocsize = 10;
                    xids = palloc(allocsize * sizeof(TransactionId));
                } else {
                    allocsize = allocsize * 2;
                    xids = repalloc(xids, allocsize * sizeof(TransactionId));
                }
            }
            xids[nxids++] = xid;
        }

        pfree(buf);
    }
    LWLockRelease(TwoPhaseStateLock);

    // Return collected XIDs if requested
    if (xids_p) {
        *xids_p = xids;
        *nxids_p = nxids;
    }

    return result;
}
```

Key simplifications made:
- Removed detailed comments about PITR recovery and XID handling
- Consolidated variable declarations for better readability
- Simplified the array growth logic while preserving the doubling strategy
- Focused on the core algorithm: scan, validate, collect minimum XID
- Preserved essential memory management and locking semantics
- Maintained the optional XID collection functionality