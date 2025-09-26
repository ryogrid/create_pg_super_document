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
  - LWLockAcquire (for ProcArrayLock exclusive access)
  - TransactionIdRetreat (decrements xid to get latestXid)
  - MaintainLatestCompletedXidRecovery (advances latestCompletedXid)
  - TransactionIdPrecedes (compares lastOverflowedXid with xid)
  - KnownAssignedXidsRemovePreceding (removes entries preceding xid)
  - LWLockRelease (releases ProcArrayLock)
- Called from (representative examples):
  - ProcArrayApplyRecoveryInfo (during recovery info processing)

## Notes and Other Information
- Similar to ProcArrayEndTransaction but designed for recovery scenarios
- Conditionally resets lastOverflowedXid only if all potentially overflowing transactions are being removed
- The conditional reset of lastOverflowedXid prevents incorrect suboverflow markings in future snapshots
- Uses exclusive locking on ProcArrayLock to ensure consistency during updates
- Part of PostgreSQL's Hot Standby recovery mechanism for managing transaction visibility