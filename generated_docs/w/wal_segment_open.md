# wal_segment_open

## Location
[src/backend/access/transam/xlogutils.c:817-841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L817-L841)

## Overview
Opens a WAL segment file for reading as part of the XLogReaderRoutine callback mechanism for local pg_wal file access.

## Definition
```c
void wal_segment_open(XLogReaderState *state, XLogSegNo nextSegNo,
                     TimeLineID *tli_p)
```

## Detailed Description
This function serves as the segment_open callback for XLogReaderState when reading local WAL files from the pg_wal directory. It constructs the appropriate file path for the requested WAL segment and opens it for reading using PostgreSQL's file I/O routines.

The function handles two types of errors:
- ENOENT: Indicates the requested WAL segment has been removed (e.g., by checkpoint cleanup)
- Other errors: General file access failures (permissions, I/O errors, etc.)

Both error conditions result in ERROR-level reports that terminate the current operation, as WAL segment accessibility is critical for database operations.

## Parameters / Member Variables
- `state`: XLogReaderState that will store the opened file descriptor and segment information
- `nextSegNo`: WAL segment number to open (XLogSegNo type, essentially uint64)
- `tli_p`: Pointer to TimeLineID specifying which timeline the segment belongs to

## Dependencies
- Functions called/Symbols referenced:
  - XLogFilePath (constructs WAL segment file path)
  - BasicOpenFile (PostgreSQL file opening routine)
  - PG_BINARY (binary file mode flag)
  - XLogSegNo (WAL segment number type)
- Called from (representative examples):
  - [XlogReadTwoPhaseData](../X/XlogReadTwoPhaseData.md)
  - [SummarizeWAL](../S/SummarizeWAL.md)
  - LogicalReplicationSlotHasPendingWal
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)

## Notes and Other Information
- Uses BasicOpenFile with O_RDONLY | PG_BINARY flags for read-only binary access
- File descriptor is stored in state->seg.ws_file for subsequent read operations
- Error handling distinguishes between missing files and other I/O failures
- [Path](../P/Path.md) construction uses the timeline ID and segment size from the XLogReaderState context
- This callback is specifically designed for local WAL file access, as opposed to streaming or archive recovery
- The opened file remains in the XLogReaderState until closed by wal_segment_close