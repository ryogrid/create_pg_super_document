# reserve_wal_for_local_slot

## Location
src/backend/replication/logical/slotsync.c: 474 - 544

## Overview
Reserves WAL segments for a newly created local synchronized slot by setting its restart_lsn and ensuring the required WAL is available.

## Definition
```c
static void reserve_wal_for_local_slot(XLogRecPtr restart_lsn)
```

## Detailed Description
This function ensures that WAL segments needed by a local synchronized slot are preserved from garbage collection. It sets the slot's restart_lsn and triggers WAL retention calculation, but handles the case where the requested WAL location has already been removed.

The function implements a retry mechanism:
1. Sets the desired restart_lsn on the slot
2. Updates system-wide WAL retention requirements
3. Checks if the required WAL segment still exists
4. If not available, retries with the oldest available WAL segment

The function determines the oldest available WAL segment by first checking the last removed segment number, and if no segments have been removed since startup, it searches the current timeline's WAL directory for the oldest existing segment.

## Parameters / Member Variables
- : The desired WAL location from which the slot should start consuming

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - SpinLockAcquire
  - SpinLockRelease
  - ReplicationSlotsComputeRequiredLSN
  - XLByteToSeg
  - XLogGetLastRemovedSegno
  - GetWalRcvFlushRecPtr
  - XLogGetOldestSegno
  - XLogSegNoOffsetToRecPtr
  - elog
- Called from:
  - synchronize_one_slot

## Notes and Other Information
- Operates on MyReplicationSlot (the currently active slot)
- Asserts that the slot's restart_lsn is initially invalid
- Uses a retry loop that typically executes at most twice
- Logs debug information about segment numbers for troubleshooting
- Currently focuses on the current timeline for WAL segment searches
- The retry mechanism handles race conditions with concurrent WAL removal
- Essential for preventing WAL segments from being removed before slot synchronization completes