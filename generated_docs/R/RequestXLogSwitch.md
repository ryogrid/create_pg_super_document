# RequestXLogSwitch

## Location
src/backend/access/transam/xlog.c: 8076 - 8093

## Overview
RequestXLogSwitch forces a WAL (Write-Ahead Log) segment switch by writing an XLOG SWITCH record, which ensures the current WAL segment is closed and a new one is started.

## Definition
```c
XLogRecPtr RequestXLogSwitch(bool mark_unimportant)
```

## Detailed Description
RequestXLogSwitch creates an XLOG SWITCH record that triggers a WAL segment switch. The function acts as a wrapper around the XLogInsert mechanism, where all the actual switching logic is implemented. When called, it forces PostgreSQL to close the current WAL segment and start writing to a new segment, regardless of whether the current segment is full. This is useful for backup operations, archiving, and administrative tasks that require clean segment boundaries.

The function optionally marks the switch record as unimportant, which affects how the record is treated during WAL processing and recovery scenarios.

## Parameters / Member Variables
- `mark_unimportant`: If true, marks the XLOG SWITCH record with XLOG_MARK_UNIMPORTANT flag, indicating this switch is not critical for consistency and can be treated with lower priority during recovery

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogInsert](../X/XLogInsert.md)
  - XLOG_MARK_UNIMPORTANT (flag)
  - XLOG_SWITCH (record type)
- Called from (representative examples):
  - [ShutdownXLOG](../S/ShutdownXLOG.md) (during database shutdown)
  - [do_pg_backup_start](../d/do_pg_backup_start.md) (during backup initiation)
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md) (during backup completion)
  - [pg_switch_wal](../p/pg_switch_wal.md) (SQL function implementation)
  - [CheckArchiveTimeout](../C/CheckArchiveTimeout.md) (during archive timeout handling)

## Notes and Other Information
- The return value is the LSN (Log Sequence Number) pointing to the end+1 address of the switch record, or the end+1 address of the prior segment if no switch was needed because we're already at a segment boundary
- XLOG SWITCH records contain no data payload - they exist solely to trigger the segment switch
- This function is commonly used in backup and archiving workflows to ensure clean segment boundaries
- The actual segment switching logic is handled internally by XLogInsert, making this function a simple interface wrapper