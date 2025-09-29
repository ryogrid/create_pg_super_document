# ExpireOldKnownAssignedTransactionIds

## Location
[src/backend/storage/ipc/procarray.c:4531-4562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4531-L4562)

## Overview
Removes KnownAssignedXids entries that precede a given transaction ID and conditionally resets the lastOverflowedXid tracking variable during recovery processing.

## Definition
```c
void ExpireOldKnownAssignedTransactionIds(TransactionId xid)
```

## Detailed Description
This function removes old entries from the KnownAssignedXids data structure by eliminating all transaction IDs that precede the specified xid parameter. It updates the latestCompletedXid to xid-1, increments the transaction completion counter, and conditionally resets the lastOverflowedXid if all potentially running transactions that might have caused overflow are being removed. This helps maintain an accurate view of active transactions during recovery and prevents incorrect suboverflow markings in snapshots.

## Parameters / Member Variables
- `xid`: The transaction ID threshold; all KnownAssignedXids entries preceding this XID will be removed

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (for ProcArrayLock exclusive access)
  - TransactionIdRetreat (decrements xid to get latestXid)
  - [MaintainLatestCompletedXidRecovery](../M/MaintainLatestCompletedXidRecovery.md) (advances latestCompletedXid)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (compares lastOverflowedXid with xid)
  - [KnownAssignedXidsRemovePreceding](../K/KnownAssignedXidsRemovePreceding.md) (removes entries preceding xid)
  - [LWLockRelease](../L/LWLockRelease.md) (releases ProcArrayLock)
- Called from (representative examples):
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md) (during recovery info processing)

## Notes and Other Information
- Similar to ProcArrayEndTransaction but designed for recovery scenarios
- Conditionally resets lastOverflowedXid only if all potentially overflowing transactions are being removed
- The conditional reset of lastOverflowedXid prevents incorrect suboverflow markings in future snapshots
- Uses exclusive locking on ProcArrayLock to ensure consistency during updates
- Part of PostgreSQL's Hot Standby recovery mechanism for managing transaction visibility

## Simplified Source

```c
// Simplified version of ExpireOldKnownAssignedTransactionIds
void ExpireOldKnownAssignedTransactionIds(TransactionId xid) {
    TransactionId latestXid;

    // Acquire exclusive lock for consistent updates
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    // Update latest completed transaction ID to xid-1
    latestXid = xid;
    TransactionIdRetreat(latestXid);
    MaintainLatestCompletedXidRecovery(latestXid);

    // Increment transaction completion counter
    TransamVariables->xactCompletionCount++;

    // Reset overflow tracking if all potentially overflowing transactions are gone
    if (TransactionIdPrecedes(procArray->lastOverflowedXid, xid)) {
        procArray->lastOverflowedXid = InvalidTransactionId;
    }

    // Remove all known assigned XIDs preceding the threshold
    KnownAssignedXidsRemovePreceding(xid);

    // Release the lock
    LWLockRelease(ProcArrayLock);
}
```

Key simplifications made:
- Added descriptive comments for each major operation
- Preserved the essential lock acquisition and release pattern
- Maintained the core logic flow for transaction ID management
- Kept the conditional overflow reset logic intact
- Simplified variable naming explanations in comments