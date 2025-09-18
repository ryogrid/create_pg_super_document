# WALAvailability

## Location
src/include/access/xlog.h: 194 - 283

## Overview
An enumeration that represents the availability status of WAL (Write-Ahead Log) segments, used to track whether specific WAL segments are still available on disk.

## Definition


## Detailed Description
WALAvailability provides status codes returned by the GetWALAvailability function to indicate the current availability of WAL segments. This is crucial for replication and backup systems that need to know if specific WAL segments are still accessible. WALAVAIL_RESERVED indicates the segment is protected by max_wal_size configuration, WALAVAIL_EXTENDED means it's kept by replication slots or wal_keep_size setting, WALAVAIL_UNRESERVED means it's no longer protected but not yet deleted, and WALAVAIL_REMOVED indicates the segment has been cleaned up and is no longer available.

## Parameters / Member Variables
- : Invalid LSN parameter provided to the function
- : WAL segment is within the max_wal_size limit and protected
- : WAL segment is reserved by a replication slot or wal_keep_size
- : WAL segment is no longer reserved but not yet removed
- : WAL segment has been removed from disk

## Dependencies
- Functions called/Symbols referenced:
  - None (enum type definition)
- Called from (representative examples):
  - CreateRestartPoint (src/backend/access/transam/xlog.c:7880)
  - PG_GET_REPLICATION_SLOTS_COLS (src/backend/replication/slotfuncs.c:264)
  - GetWALAvailability function (referenced in src/include/access/xlog.h:242)

## Notes and Other Information
- Essential for replication monitoring and troubleshooting WAL segment availability issues
- Used by replication slot management to determine if required WAL is still available
- Helps in diagnosing replication lag and WAL retention problems
- Critical for backup and recovery operations that depend on WAL segment availability
- The availability status affects whether replication can continue or needs to be reinitialized