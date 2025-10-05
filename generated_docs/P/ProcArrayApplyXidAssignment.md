# ProcArrayApplyXidAssignment

## Location
[src/backend/storage/ipc/procarray.c:1318-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L1318-L1401)

## Overview
ProcArrayApplyXidAssignment processes XLOG_XACT_ASSIGNMENT WAL records during recovery to maintain subtransaction parent-child relationships on standby servers.

## Definition
```c
void ProcArrayApplyXidAssignment(TransactionId topxid, int nsubxids, TransactionId *subxids)
```

## Detailed Description
This function handles the replay of transaction assignment records during PostgreSQL recovery on standby servers. When a primary server assigns subtransactions to a top-level transaction, it writes an XLOG_XACT_ASSIGNMENT record that must be replayed on standbys to maintain consistent subtransaction state.

The function performs several critical operations:
1. Records all subtransactions as observed by calling RecordKnownAssignedTransactionIds with the maximum XID
2. Establishes parent-child relationships in pg_subtrans by directly linking each subtransaction to the top-level transaction (bypassing intermediate levels)
3. Removes the assigned subtransactions from KnownAssignedXids since they are no longer independent transactions
4. Updates lastOverflowedXid to track the highest assigned subtransaction ID

A key difference from normal processing is that during recovery, subtransactions are linked directly to the top-level transaction rather than maintaining the full subtransaction tree hierarchy. This optimization is safe because aborted subtransactions are already marked in clog, eliminating the need to traverse intermediate transaction states.

## Parameters / Member Variables
- `topxid`: Transaction ID of the top-level (parent) transaction that is being assigned subtransactions
- `nsubxids`: Number of subtransaction IDs in the subxids array
- `subxids`: Array of subtransaction IDs being assigned to the top-level transaction

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdLatest](../T/TransactionIdLatest.md) (to find the maximum XID among all transactions)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md) (to mark subtransactions as observed)
  - [SubTransSetParent](../S/SubTransSetParent.md) (to establish parent-child relationships in pg_subtrans)
  - [KnownAssignedXidsRemoveTree](../K/KnownAssignedXidsRemoveTree.md) (to remove assigned subtransactions from known assignments)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (for XID comparison and ordering)
- Called from:
  - [xact_redo](../x/xact_redo.md) (during transaction-related WAL record replay)

## Notes and Other Information
- Must be called with standbyState >= STANDBY_INITIALIZED
- Only operates on KnownAssignedXids when standbyState > STANDBY_INITIALIZED
- Uses exclusive ProcArrayLock during KnownAssignedXids manipulation
- Assumes that assignment records contain at most PGPROC_MAX_CACHED_SUBXIDS entries
- Critical for maintaining consistent subtransaction visibility on Hot Standby servers
- The direct parent-child linking (bypassing intermediate levels) is a recovery-specific optimization
- Updates lastOverflowedXid to ensure proper snapshot overflow tracking

## Simplified Source

```c
void ProcArrayApplyXidAssignment(TransactionId topxid, int nsubxids, TransactionId *subxids) {
    Assert(standbyState >= STANDBY_INITIALIZED);

    // Find the highest XID among all transactions
    TransactionId max_xid = TransactionIdLatest(topxid, nsubxids, subxids);

    // Mark all subtransactions as observed
    RecordKnownAssignedTransactionIds(max_xid);

    // Link each subtransaction directly to the top-level transaction
    // (Recovery optimization: skip intermediate subtransaction levels)
    for (int i = 0; i < nsubxids; i++) {
        SubTransSetParent(subxids[i], topxid);
    }

    // Early return if KnownAssignedXids isn't maintained yet
    if (standbyState == STANDBY_INITIALIZED)
        return;

    // Remove assigned subtransactions from known-assigned-xacts
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    KnownAssignedXidsRemoveTree(InvalidTransactionId, nsubxids, subxids);

    // Update overflow tracking to the highest assigned XID
    if (TransactionIdPrecedes(procArray->lastOverflowedXid, max_xid))
        procArray->lastOverflowedXid = max_xid;

    LWLockRelease(ProcArrayLock);
}
```