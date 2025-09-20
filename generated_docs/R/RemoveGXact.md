# RemoveGXact

## Location
[src/backend/access/transam/twophase.c:628-665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L628-L665)

## Overview
RemoveGXact removes a prepared transaction from the shared memory array and returns it to the freelist for reuse.

## Definition

```c
static void
RemoveGXact(GlobalTransaction gxact)
```
## Detailed Description
RemoveGXact is a static function in the two-phase commit system that manages the lifecycle of prepared transactions in shared memory. It removes a specified GlobalTransaction from the active prepared transactions array (TwoPhaseState->prepXacts) and returns it to the freelist for future use. The function performs array compaction by moving the last element to fill the gap left by the removed transaction, maintaining array density. The caller must have already removed the transaction from ProcArray before calling this function, and must hold TwoPhaseStateLock in exclusive mode.

## Parameters / Member Variables
- : The GlobalTransaction structure to be removed from the prepared transactions array

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode (for lock assertion)
  - elog (for error reporting)
- Global structures accessed:
  - TwoPhaseState (global two-phase commit state)
  - GlobalTransaction (transaction structure type)
- Called from:
  - [AtAbort_Twophase](../A/AtAbort_Twophase.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [PrepareRedoRemove](../P/PrepareRedoRemove.md)

## Notes and Other Information
- The function requires TwoPhaseStateLock to be held in exclusive mode by the caller
- Uses array compaction technique: moves the last element to fill the removed element's position
- Returns the removed GlobalTransaction to the freelist for memory reuse
- Will throw an ERROR if the specified gxact is not found in the active array
- This is an internal function (static) used only within the two-phase commit subsystem