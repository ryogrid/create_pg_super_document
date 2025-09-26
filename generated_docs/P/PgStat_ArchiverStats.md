# PgStat_ArchiverStats

## Location
[src/include/pgstat.h:240-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L240-L251)

## Overview
PgStat_ArchiverStats tracks statistics for PostgreSQL's WAL (Write-Ahead Logging) archiver process, maintaining counters and metadata for both successful and failed archival operations.

## Definition

```c
typedef struct PgStat_ArchiverStats
{
	PgStat_Counter archived_count;	/* archival successes */
	char		last_archived_wal[MAX_XFN_CHARS + 1];	/* last WAL file
														 * archived */
	TimestampTz last_archived_timestamp;	/* last archival success time */
	PgStat_Counter failed_count;	/* failed archival attempts */
	char		last_failed_wal[MAX_XFN_CHARS + 1]; /* WAL file involved in
													 * last failure */
	TimestampTz last_failed_timestamp;	/* last archival failure time */
	TimestampTz stat_reset_timestamp;
} PgStat_ArchiverStats;
```
## Detailed Description
PgStat_ArchiverStats is a statistical tracking structure for PostgreSQL's WAL archiver process. The archiver is responsible for copying completed WAL segment files to a safe, typically remote location as part of PostgreSQL's point-in-time recovery (PITR) and backup strategy. This structure maintains comprehensive statistics including success and failure counts, information about the most recently processed WAL files, and timestamps for monitoring archiver health and performance. The structure supports administrative queries and monitoring tools that need to track archiver activity.

## Parameters / Member Variables
- : Counter tracking the total number of successful WAL file archival operations
- : Character array storing the filename of the most recently successfully archived WAL file (up to MAX_XFN_CHARS characters)
- : Timestamp indicating when the last successful archival operation completed
- : Counter tracking the total number of failed archival attempts
- : Character array storing the filename of the WAL file involved in the most recent archival failure
- : Timestamp indicating when the last archival failure occurred
- : Timestamp marking when these archiver statistics were last reset to zero

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - MAX_XFN_CHARS
  - TimestampTz
- Called from (representative examples):
  - pgstat_report_archiver
  - pgstat_archiver_snapshot_cb
  - pg_stat_get_archiver
  - PgStatShared_Archiver
  - PgStat_Snapshot

## Notes and Other Information
This structure is essential for monitoring PostgreSQL's backup and recovery infrastructure. The archiver statistics are accessible through the pg_stat_archiver system view, allowing database administrators to monitor the health and performance of their archival process. Failed archival operations can indicate issues with backup infrastructure, storage availability, or network connectivity that could impact recovery capabilities. The MAX_XFN_CHARS constant determines the maximum length for WAL filenames that can be stored in the statistics.