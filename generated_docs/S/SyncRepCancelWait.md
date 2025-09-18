# SyncRepCancelWait

## Location
src/backend/replication/syncrep.c: 406 - 415

## Overview
Cancels any synchronous replication wait currently in progress by removing the process from the wait queue and resetting its synchronization state.

## Definition
```c
static void SyncRepCancelWait(void)
```

## Detailed Description
This static function provides a safe mechanism to abort a synchronous replication wait operation. It acquires the SyncRepLock exclusively to ensure atomic removal from the wait queue and state updates. The function checks if the current process is actually queued (using dlist_node_is_detached) before attempting removal, making it safe to call even when the process is not waiting.

The function performs a thorough deletion from the doubly-linked list queue, which not only removes the node but also reinitializes its links to prevent dangling pointers. After removal, it resets the process's synchronization state to SYNC_REP_NOT_WAITING, effectively canceling the wait operation.

This function is typically called during error conditions, process termination, or user-requested cancellations when it's no longer appropriate or possible to continue waiting for replication confirmation.

## Parameters / Member Variables
- None (operates on the current process MyProc)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (acquires SyncRepLock exclusively)
  - LWLockRelease (releases SyncRepLock)
  - [dlist_node_is_detached](../d/dlist_node_is_detached.md) (checks if node is in a list)
  - [dlist_delete_thoroughly](../d/dlist_delete_thoroughly.md) (removes and reinitializes list node)
  - SYNC_REP_NOT_WAITING (synchronization state constant)
- Called from (representative examples):
  - [SyncRepWaitForLSN](SyncRepWaitForLSN.md) (multiple call sites for different cancellation scenarios)
  - SyncStandbysDefined

## Notes and Other Information
The function uses dlist_delete_thoroughly instead of simple deletion to ensure the list node links are properly reinitialized, preventing potential issues with subsequent operations. The exclusive lock ensures that queue modifications are atomic with respect to WAL sender processes that may be concurrently processing the queue. This function is essential for graceful handling of interrupted synchronous replication waits.