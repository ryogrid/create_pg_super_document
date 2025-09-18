# pgstat_report_archiver

## Location
src/backend/utils/activity/pgstat_archiver.c: 28 - 57

## Overview
Reports archiver statistics to the shared memory statistics collector, tracking successful archival operations and failed attempts with corresponding WAL file names and timestamps.

## Definition
```c
void pgstat_report_archiver(const char *xlog, bool failed)
```

## Detailed Description
This function updates the archiver statistics in shared memory based on the outcome of a WAL archival operation. It uses a changecount mechanism to ensure atomic updates to shared statistics. When an archive operation completes (successfully or with failure), this function records the outcome along with the WAL file name and current timestamp. The statistics include separate counters and metadata for successful and failed operations.

## Parameters / Member Variables
- `xlog`: Name of the WAL file that was archived or failed to archive
- `failed`: Boolean flag indicating whether the archival operation failed (true) or succeeded (false)

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Archiver
  - GetCurrentTimestamp
  - pgstat_begin_changecount_write
  - pgstat_end_changecount_write
  - PgStat_ArchiverStats
- Called from (representative examples):
  - pgarch_ArchiverCopyLoop

## Notes and Other Information
The function uses the changecount mechanism to ensure atomic updates to shared memory statistics, preventing readers from seeing inconsistent intermediate states during updates. The statistics are maintained separately for successful and failed archival attempts, with each category tracking count, last WAL file name, and timestamp.