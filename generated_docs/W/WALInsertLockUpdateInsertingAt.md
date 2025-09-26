# WALInsertLockUpdateInsertingAt

## Location
[src/backend/access/transam/xlog.c:1473-1505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1473-L1505)

## Overview
Updates the insertingAt position variable for WAL insertion locks to signal completion of WAL insertion up to a specific point.

## Definition
```c
static void WALInsertLockUpdateInsertingAt(XLogRecPtr insertingAt)
```

## Detailed Description
This function updates the `insertingAt` variable associated with WAL insertion locks to inform other backends about the progress of WAL insertion. The `insertingAt` value represents the highest WAL position that has been successfully inserted by the current backend.

The function handles two different scenarios based on the current lock state:

1. **Exclusive Lock Mode**: When `holdingAllLocks` is true (indicating all WAL insertion locks are held exclusively), the function updates only the last lock's `insertingAt` variable. This design choice follows the pattern established in `WALInsertLockAcquireExclusive`, where all locks except the last one have their `insertingAt` set to maximum values.

2. **Single Lock Mode**: In normal operation, when holding only a single WAL insertion lock, the function updates the `insertingAt` variable for the lock identified by `MyLockNo`.

This mechanism allows other backends to track the progress of WAL insertion and coordinate their operations accordingly, particularly when waiting for specific WAL positions to be written.

## Parameters / Member Variables
- `insertingAt`: XLogRecPtr value indicating the WAL position up to which insertion has been completed

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockUpdateVar](../L/LWLockUpdateVar.md) (updates the insertingAt variable associated with the lock)
  - NUM_XLOGINSERT_LOCKS (constant defining number of WAL insertion locks)
  - holdingAllLocks (global flag indicating exclusive lock ownership)
  - MyLockNo (backend-specific lock identifier)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [GetXLogBuffer](../G/GetXLogBuffer.md)

## Notes and Other Information
- This is a static function, only accessible within the xlog.c module
- The function coordinates with the locking strategy used by WALInsertLockAcquireExclusive
- When holding all locks, only the last lock's position is updated with the real value
- Essential for other backends to determine when it's safe to proceed with WAL-dependent operations
- The insertingAt value is used by LWLockWaitForVar to implement efficient waiting mechanisms