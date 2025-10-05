# pgstat_report_archiver

## Location
[src/backend/utils/activity/pgstat_archiver.c:28-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_archiver.c#L28-L57)

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
  - [PgStatShared_Archiver](../P/PgStatShared_Archiver.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_begin_changecount_write](pgstat_begin_changecount_write.md)
  - [pgstat_end_changecount_write](pgstat_end_changecount_write.md)
  - [PgStat_ArchiverStats](../P/PgStat_ArchiverStats.md)
- Called from (representative examples):
  - [pgarch_ArchiverCopyLoop](pgarch_ArchiverCopyLoop.md)

## Notes and Other Information
The function uses the changecount mechanism to ensure atomic updates to shared memory statistics, preventing readers from seeing inconsistent intermediate states during updates. The statistics are maintained separately for successful and failed archival attempts, with each category tracking count, last WAL file name, and timestamp.

## Simplified Source

```c
void pgstat_report_archiver(const char *xlog, bool failed) {
    PgStatShared_Archiver *stats_shmem = &pgStatLocal.shmem->archiver;
    TimestampTz now = GetCurrentTimestamp();

    // Begin atomic update
    pgstat_begin_changecount_write(&stats_shmem->changecount);

    if (failed) {
        // Record failed archival
        ++stats_shmem->stats.failed_count;
        memcpy(&stats_shmem->stats.last_failed_wal, xlog,
               sizeof(stats_shmem->stats.last_failed_wal));
        stats_shmem->stats.last_failed_timestamp = now;
    } else {
        // Record successful archival
        ++stats_shmem->stats.archived_count;
        memcpy(&stats_shmem->stats.last_archived_wal, xlog,
               sizeof(stats_shmem->stats.last_archived_wal));
        stats_shmem->stats.last_archived_timestamp = now;
    }

    // End atomic update
    pgstat_end_changecount_write(&stats_shmem->changecount);
}
```