# pgstat_io_reset_all_cb

## Location
src/backend/utils/activity/pgstat_io.c: 255 - 276

## Overview
Resets all I/O statistics across all backend types and updates the reset timestamp.

## Definition


## Detailed Description
This function serves as a callback to reset all I/O statistics stored in shared memory across all backend types. It iterates through each backend type, acquires the appropriate exclusive lock, and clears the statistics data using memset. The function also updates the statistics reset timestamp using the first backend type's lock for synchronization. This ensures that all I/O statistics are atomically reset and the reset time is properly recorded for reference.

## Parameters / Member Variables
- `ts`: Timestamp value to set as the new statistics reset timestamp

## Dependencies
- Functions called/Symbols referenced:
  - BACKEND_NUM_TYPES
  - LWLock
  - PgStat_BktypeIO
- Called from (representative examples):
  - SH_DECLARE (as part of statistics system callbacks)

## Notes and Other Information
- Uses exclusive lightweight locks to ensure thread-safe reset operations
- Resets statistics for all backend types in a loop from 0 to BACKEND_NUM_TYPES-1
- Sets the reset timestamp only when processing the first backend type (i == 0)
- Clears all statistics data structures using memset to zero them out completely
- This is a callback function used by PostgreSQL's statistics reset infrastructure
- Located in src/backend/utils/activity/pgstat_io.c:255-276