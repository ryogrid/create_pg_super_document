# GetConflictingVirtualXIDs

## Location
src/backend/storage/ipc/procarray.c: 3416 - 3489

## Overview
Returns an array of currently active Virtual Transaction IDs (VXIDs) that may conflict with recovery operations, specifically designed for conflict resolution during recovery on standby servers.

## Definition


## Detailed Description
GetConflictingVirtualXIDs is specialized for hot standby conflict resolution scenarios where recovery processes need to identify active transactions that might conflict with cleanup operations. Unlike GetCurrentVirtualXIDs, this function is specifically optimized for recovery conflict detection and uses different filtering logic.

The function uses a static array that is allocated once with malloc (rather than palloc) to minimize overhead from repeated allocation/deallocation during recovery operations. The result includes a terminator entry for easy iteration. The function excludes prepared transactions (processes with pid == 0) and can optionally filter by database OID.

The conflict detection logic is based on transaction snapshot horizons - transactions with xmin values that could potentially see data being cleaned up by recovery operations are considered conflicting. The function works under shared ProcArrayLock to allow concurrent snapshot taking while ensuring consistency.

## Parameters / Member Variables
- `limitXmin`: Transaction ID cutoff for conflict detection; transactions with xmin ≤ limitXmin may conflict. If InvalidTransactionId, all transactions are considered conflicting
- `dbOid`: Database OID filter; if valid, only includes transactions from this database. If invalid, includes all databases

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - LWLockAcquire/LWLockRelease
  - TransactionIdFollows
  - GET_VXID_FROM_PGPROC
  - VirtualTransactionIdIsValid
  - UINT32_ACCESS_ONCE
- Called from (representative examples):
  - ResolveRecoveryConflictWithSnapshot (in storage/ipc/standby.c)
  - ResolveRecoveryConflictWithTablespace (in storage/ipc/standby.c)

## Notes and Other Information
- Uses static allocation with malloc instead of palloc for performance reasons during recovery
- **IMPORTANT**: Callers must NOT pfree the result - the array is reused across calls
- The result array includes a terminator entry with INVALID_PROC_NUMBER and InvalidLocalTransactionId
- Excludes prepared transactions (pid == 0) from conflict detection
- Designed specifically for recovery conflict resolution, not general transaction monitoring
- Uses shared locking to allow concurrent snapshot operations while ensuring consistency
- Invalid pxmin values are ignored because backends without snapshots cannot conflict with cleanup operations
- The function's conflict detection logic is optimized for cleanup records that remove tuple versions from committed transactions