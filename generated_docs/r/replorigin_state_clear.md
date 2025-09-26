# replorigin_state_clear

## Location
src/backend/replication/logical/origin.c: 341 - 410

## Overview
A static helper function that clears the in-memory replication state for a given replication origin, handling concurrency and creating appropriate WAL log entries during the cleanup process.

## Definition

```c
static void
replorigin_state_clear(RepOriginId roident, bool nowait)
```
## Detailed Description
This function cleans up the in-memory replication state associated with a specific replication origin ID. It searches through the global replication_states array to find the matching slot and handles various concurrency scenarios. When the slot is currently in use (acquired_by != 0), the function either waits for it to become available or throws an error based on the nowait parameter. Upon finding an available slot, it logs a WAL record (XLOG_REPLORIGIN_DROP) to ensure crash recovery consistency, then clears the slot's state fields (roident, remote_lsn, local_lsn). The function uses exclusive locking and condition variables to ensure thread-safe operation and proper synchronization.

## Parameters / Member Variables
- : The replication origin identifier whose state should be cleared
- : Boolean flag controlling behavior when the slot is busy (true = throw error immediately, false = wait for availability)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - ereport
  - errcode
  - errmsg
  - ConditionVariableSleep
  - ConditionVariableCancelSleep
  - XLogBeginInsert
  - XLogRegisterData
  - XLogInsert
  - ReplicationState
  - ConditionVariable
  - xl_replorigin_drop
  - RepOriginId
  - XLOG_REPLORIGIN_DROP
  - InvalidRepOriginId
  - InvalidXLogRecPtr
- Called from (representative examples):
  - replorigin_drop_by_name

## Notes and Other Information
- This is a static function, accessible only within the origin.c file
- Uses a restart mechanism with goto to handle cases where waiting is required
- Acquires ReplicationOriginLock in exclusive mode to ensure atomic operations
- Creates WAL log entries to ensure that replication origin drops are crash-safe
- Handles the case where a replication slot is currently in use by another process
- When nowait is true and slot is busy, throws ERRCODE_OBJECT_IN_USE error
- When nowait is false, uses condition variable waiting mechanism with WAIT_EVENT_REPLICATION_ORIGIN_DROP
- Clears all relevant state fields including roident, remote_lsn, and local_lsn
- Always calls ConditionVariableCancelSleep to clean up any pending sleep operations