# pgstat_archiver_reset_all_cb

## Location
src/backend/utils/activity/pgstat_archiver.c: 66 - 80

## Overview
Callback function that resets archiver statistics by creating a baseline snapshot of current values and setting the reset timestamp.

## Definition
```c
void pgstat_archiver_reset_all_cb(TimestampTz ts)
```

## Detailed Description
This callback function implements the reset functionality for archiver statistics. Rather than zeroing out all statistics, it follows PostgreSQL's reset protocol by creating a "reset offset" - a snapshot of the current statistics values that will be subtracted from future readings to show incremental changes since the reset. The function acquires an exclusive lock on the archiver statistics, copies the current statistics to the reset_offset structure, and updates the reset timestamp. This allows statistics to continue accumulating in shared memory while SQL functions can report delta values since the last reset.

## Parameters / Member Variables
- `ts`: Timestamp when the reset operation was initiated

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Archiver
  - pgstat_copy_changecounted_stats
- Called from (representative examples):
  - SH_DECLARE (via statistics framework callback mechanism)

## Notes and Other Information
The reset protocol preserves historical statistics in shared memory while providing a way to report incremental values since the last reset. This approach is more robust than clearing statistics entirely, as it prevents loss of data during concurrent operations and allows for proper accounting of activities that span reset operations.