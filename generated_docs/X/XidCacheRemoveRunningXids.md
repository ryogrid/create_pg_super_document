# XidCacheRemoveRunningXids

## Location
src/backend/storage/ipc/procarray.c: 3990 - 4077

## Overview
Removes a group of transaction IDs from the list of known-running subtransactions for the current backend, typically used during transaction abort operations.

## Definition
```c
void XidCacheRemoveRunningXids(TransactionId xid, int nxids, const TransactionId *xids, TransactionId latestXid)
```

## Detailed Description
This function removes specified transaction IDs from the current backend's subtransaction cache. It is primarily used during transaction abort operations to clean up the list of running subtransactions. The function operates on both a primary transaction ID and an array of subtransaction IDs, removing all of them from the cache.

The function performs several optimizations:
- Scans backwards through arrays to achieve better performance when removing many XIDs
- Uses memory barriers to ensure proper ordering of writes
- Updates global transaction completion tracking
- Handles cache overflow scenarios gracefully

The removal process maintains consistency in the global process array while ensuring other backends can observe the changes correctly.

## Parameters / Member Variables
- `xid`: The primary transaction ID to remove from the subtransaction cache
- `nxids`: The number of subtransaction IDs in the xids array
- `xids`: Array of subtransaction IDs to remove from the cache
- `latestXid`: The latest (highest) transaction ID among the group being removed

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdEquals (for XID comparison)
  - LWLockAcquire/LWLockRelease (for exclusive process array locking)
  - pg_write_barrier (for memory ordering)
  - MaintainLatestCompletedXid (for global completion tracking)
  - elog (for warning messages)
- Called from (representative examples):
  - RecordTransactionAbort (in xact.c)

## Notes and Other Information
- Requires exclusive ProcArrayLock to ensure atomic removal operations
- Handles cache overflow scenarios where XIDs may not be found in the cache
- Uses backward scanning for O(N) instead of O(N²) performance when removing many XIDs
- May issue warnings if XIDs are not found, but this can be normal in error recovery scenarios
- Updates global transaction completion count to notify other processes
- Memory barriers ensure proper visibility of changes to other backends
- Function can be called multiple times for the same subtransaction during error recovery