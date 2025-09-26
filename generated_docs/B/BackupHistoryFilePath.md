# BackupHistoryFilePath

## Location
[src/include/access/xlog_internal.h:261-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L261-L272)

## Overview
BackupHistoryFilePath is a static inline function that constructs the complete file system path for backup history files in PostgreSQL's WAL directory.

## Definition

```c
typedef struct xl_parameter_change
{
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	int			wal_level;
	bool		wal_log_hints;
	bool		track_commit_timestamp;
} xl_parameter_change;
```
## Detailed Description
This function generates the full file system path for backup history files by combining the XLOGDIR directory path with a standardized backup history filename. It works as a companion to BackupHistoryFileName, but provides the complete path including the WAL directory prefix. The path construction ensures that backup history files are properly located within the PostgreSQL data directory structure, making them accessible to WAL management processes.

## Parameters / Member Variables
- `path`: Output buffer where the constructed file path will be stored (must accommodate MAXPGPATH length)
- `tli`: TimeLineID identifying the timeline for this backup history file
- `logSegNo`: XLogSegNo specifying the WAL segment number
- `startpoint`: XLogRecPtr indicating the starting position within the WAL segment
- `wal_segsz_bytes`: Size of WAL segments in bytes, used for calculating segment boundaries

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegmentsPerXLogId (called twice for segment calculations)
  - XLogSegmentOffset (for calculating offset within segment)
- Constants used:
  - MAXPGPATH (maximum path length)
  - XLOGDIR (WAL directory path)
- Called from (representative examples):
  - do_pg_backup_stop (in src/backend/access/transam/xlog.c)

## Notes and Other Information
The generated path follows the format: `{XLOGDIR}/{tli:08X}{high:08X}{low:08X}.{offset:08X}.backup` where XLOGDIR is the WAL directory path (typically 'pg_wal'). This function is essential for creating backup history files at the correct location within the PostgreSQL data directory structure, ensuring they can be found by recovery and cleanup processes.