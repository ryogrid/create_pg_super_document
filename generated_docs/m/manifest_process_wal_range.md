# manifest_process_wal_range

## Location
[src/backend/backup/basebackup_incremental.c:995-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L995-L1012)

## Overview
A callback function invoked for each WAL range mentioned in the backup manifest to collect WAL timeline and LSN information for incremental backup processing.

## Definition
```c
static void
manifest_process_wal_range(JsonManifestParseContext *context,
                          TimeLineID tli, XLogRecPtr start_lsn,
                          XLogRecPtr end_lsn)
```

## Detailed Description
This function serves as a callback during backup manifest parsing, specifically handling WAL (Write-Ahead Log) range entries. It creates backup_wal_range structures to store timeline ID and LSN (Log Sequence Number) information for each WAL range found in the manifest. The function appends each range to a list maintained in the IncrementalBackupInfo structure, allowing the backup system to track WAL coverage and determine the oldest LSN and corresponding timeline ID needed for incremental backup operations.

## Parameters / Member Variables
- `context`: JsonManifestParseContext pointer containing parsing state and private data
- `tli`: TimeLineID indicating the timeline this WAL range belongs to
- `start_lsn`: XLogRecPtr marking the beginning LSN of this WAL range
- `end_lsn`: XLogRecPtr marking the ending LSN of this WAL range

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (as callback in manifest parsing)

## Notes and Other Information
- This is a static function local to basebackup_incremental.c
- Part of the incremental backup infrastructure for tracking WAL coverage
- Each WAL range is allocated using palloc and stored in a linked list
- The collected WAL ranges help determine the minimum WAL required for backup consistency
- Timeline IDs are crucial for handling WAL across different database timelines (e.g., after point-in-time recovery)

## Simplified Source

```c
static void manifest_process_wal_range(JsonManifestParseContext *context,
                                      TimeLineID tli, XLogRecPtr start_lsn,
                                      XLogRecPtr end_lsn) {
    IncrementalBackupInfo *ib = context->private_data;

    // Create new WAL range entry
    backup_wal_range *range = palloc(sizeof(backup_wal_range));
    range->tli = tli;
    range->start_lsn = start_lsn;
    range->end_lsn = end_lsn;

    // Add to the list of manifest WAL ranges
    ib->manifest_wal_ranges = lappend(ib->manifest_wal_ranges, range);
}
```