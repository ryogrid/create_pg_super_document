# pgstat_report_tempfile

## Location
src/backend/utils/activity/pgstat_database.c: 175 - 190

## Overview
Reports the creation of a temporary file by updating database-level statistics to track the number and total size of temporary files created.

## Definition
```c
void pgstat_report_tempfile(size_t filesize)
```

## Detailed Description
This function tracks temporary file usage at the database level in PostgreSQL's statistics system. When a temporary file is created, this function increments the temporary file count and adds the file size to the cumulative bytes counter for the current database. 

The function uses a pending statistics entry approach, which means the statistics are buffered locally and will be flushed to shared memory at appropriate intervals rather than updating shared statistics immediately. This is more efficient for frequent operations like temporary file creation.

The function only operates when statistics tracking is enabled (pgstat_track_counts is true).

## Parameters / Member Variables
- `filesize`: The size of the temporary file being reported, in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_prep_database_pending
  - PgStat_StatDBEntry (data structure)
  - MyDatabaseId (global variable)
  - pgstat_track_counts (global variable)
- Called from (representative examples):
  - ReportTemporaryFileUsage (in src/backend/storage/file/fd.c:1527)

## Notes and Other Information
- Only operates when pgstat_track_counts is enabled
- Uses pending statistics approach for better performance with frequent updates
- Tracks both the count of temporary files and total bytes consumed
- Part of PostgreSQL's database-level performance monitoring system
- Temporary files are often created during large sorts, hashes, and other operations that exceed work_mem
- These statistics help administrators monitor memory pressure and tune work_mem settings