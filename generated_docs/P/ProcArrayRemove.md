# ProcArrayRemove

## Location
[src/backend/storage/ipc/procarray.c:565-666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L565-L666)

## Overview
Removes a specified PGPROC from the shared process array, maintaining the sorted order and optionally advancing transaction completion tracking for 2PC transactions.

## Definition

```c
void
ProcArrayRemove(PGPROC *proc, TransactionId latestXid)
```
## Detailed Description
ProcArrayRemove removes a process entry from the shared process array while maintaining the array's sorted order. The function handles two distinct scenarios: removing a live 2PC (two-phase commit) transaction or removing a process that is no longer active.

When removing a live 2PC transaction (indicated by a valid latestXid), the function advances the global latestCompletedXid to reflect that the transaction is no longer running. This is equivalent to calling ProcArrayEndTransaction followed by removal, but optimized to acquire locks only once.

The function performs several cleanup operations:
1. Clears transaction-related fields in the global arrays
2. Shifts remaining entries to fill the gap left by the removed process
3. Updates pgxactoff values for all shifted processes
4. Maintains the integrity of parallel arrays (xids, subxidStates, statusFlags)

## Parameters / Member Variables
- : Pointer to the PGPROC structure to be removed from the shared array
- : Transaction ID of the transaction being removed (InvalidTransactionId if not a live 2PC transaction)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - TransactionIdIsValid
  - [MaintainLatestCompletedXid](../M/MaintainLatestCompletedXid.md)
  - [DisplayXidCache](../D/DisplayXidCache.md) (debug builds only)
  - memmove
  - [ProcArrayStruct](ProcArrayStruct.md)
  - ProcGlobal
  - TransamVariables
  - NUM_AUXILIARY_PROCS

- Called from (representative examples):
  - [RemoveProcFromArray](../R/RemoveProcFromArray.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
- Requires exclusive locks on both ProcArrayLock and XidGenLock for atomic updates
- Special handling for 2PC transactions that need to advance latestCompletedXid
- Maintains sorted order of the process array by shifting entries after removal
- Updates multiple parallel arrays atomically to maintain consistency
- Includes debug functionality to display XID cache stats on backend shutdown
- Lock release order is reversed from acquisition order to minimize contention
- Sets removed entry to -1 for debugging purposes
- Validates that the process being removed has expected state (no active transaction unless it's a 2PC case)
- The function assumes the process array is already sorted and maintains this invariant