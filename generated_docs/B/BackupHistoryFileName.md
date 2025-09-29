# BackupHistoryFileName

## Location
[src/include/access/xlog_internal.h:244-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L244-L252)

## Overview
BackupHistoryFileName is a static inline function that constructs standardized backup history file names in PostgreSQL's WAL (Write-Ahead Log) system.

## Definition

```c
static inline void
BackupHistoryFileName(char *fname, TimeLineID tli, XLogSegNo logSegNo, XLogRecPtr startpoint, int wal_segsz_bytes)
```
## Detailed Description
This function generates a standardized filename for backup history files, which are used to track backup operations in PostgreSQL's WAL system. The filename follows a specific format that incorporates timeline ID, segment number components, and starting point offset, ensuring unique and descriptive naming for backup history files. The function uses a predefined format string to create names that can be easily parsed and understood by other WAL management functions.

## Parameters / Member Variables
- : Output buffer where the constructed filename will be stored
- : TimeLineID identifying the timeline for this backup history file
- : XLogSegNo specifying the WAL segment number 
- : XLogRecPtr indicating the starting position within the WAL segment
- : Size of WAL segments in bytes, used for calculating segment boundaries

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegmentsPerXLogId (called twice for segment calculations)
  - XLogSegmentOffset (for calculating offset within segment)
- Constants used:
  - MAXFNAMELEN (maximum filename length)
- Called from (representative examples):
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md) (in src/backend/access/transam/xlog.c)

## Notes and Other Information
The generated filename follows the format:  where high and low represent the segment number split across XLogId boundaries, and offset represents the position within the segment. This naming convention ensures that backup history files are uniquely identifiable and can be sorted chronologically.

## Simplified Source

```c
// Generate backup history filename from WAL coordinates
static inline void BackupHistoryFileName(char *fname, TimeLineID tli,
                                       XLogSegNo logSegNo, XLogRecPtr startpoint,
                                       int wal_segsz_bytes)
{
    // Format: timeline.high.low.offset.backup
    snprintf(fname, MAXFNAMELEN, "%08X%08X%08X.%08X.backup",
             tli,
             (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),    // high
             (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)),    // low
             (uint32) (XLogSegmentOffset(startpoint, wal_segsz_bytes)));      // offset
}
```