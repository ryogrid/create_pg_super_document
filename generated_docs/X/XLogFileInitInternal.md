# XLogFileInitInternal

## Location
[src/backend/access/transam/xlog.c:3187-3356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3187-L3356)

## Overview
Creates and initializes a new WAL file segment by either reusing an existing file or creating a zero-filled temporary file that gets atomically moved into place.

## Definition

```c
static int
XLogFileInitInternal(XLogSegNo logsegno, TimeLineID logtli,
					 bool *added, char *path)
```
## Detailed Description
XLogFileInitInternal is responsible for ensuring a specific WAL file segment exists and is properly initialized. The function implements a robust two-phase creation process:

**Phase 1 - Check for existing file:**
- First attempts to open an existing file at the target location
- If successful, returns the file descriptor immediately (checkpoint maker may have already created it)
- Uses proper sync flags based on wal_sync_method configuration

**Phase 2 - Create new file if needed:**
- Creates a temporary file with a unique name (xlogtemp.PID) to avoid conflicts
- Initializes the file content based on wal_init_zero setting:
  - If wal_init_zero=true: Zero-fills the entire segment to ensure disk space allocation
  - If wal_init_zero=false: Writes only a single byte at the end (more efficient but may create sparse files)
- Performs fsync to ensure the data reaches disk
- Atomically renames the temporary file to its final location using InstallXLogFileSegment
- Handles concurrent creation attempts gracefully by potentially using the created segment for a future slot

The function includes comprehensive error handling, wait event reporting for monitoring, and support for direct I/O optimization when configured.

## Parameters / Member Variables
- : The WAL segment number to create
- : Timeline ID for the segment
- : Output parameter set to true if a new segment was actually created
- : Output buffer (MAXPGPATH) containing the final path to the segment file
- Returns: File descriptor of opened file, or -1 (caller should open the path directly)

## Dependencies
- Functions called/Symbols referenced:
  - XLogFilePath
  - BasicOpenFile
  - [get_sync_bit](../g/get_sync_bit.md)
  - pg_pwrite_zeros
  - pg_pwrite
  - pg_fsync
  - pgstat_report_wait_start/end
  - [InstallXLogFileSegment](../I/InstallXLogFileSegment.md)
- Called from (representative examples):
  - [XLogFileInit](XLogFileInit.md)
  - [PreallocXlogFiles](../P/PreallocXlogFiles.md)

## Notes and Other Information
- Uses CheckPointSegments to determine maximum pre-created segments
- Supports direct I/O when IO_DIRECT_WAL_INIT flag is set
- Implements atomic file creation via temporary files to avoid corruption
- Handles race conditions where multiple processes may create the same segment
- The wal_init_zero setting affects both performance and disk space allocation behavior
- Returns -1 even on success; callers typically need to open the returned path
- Includes comprehensive wait event reporting for performance monitoring
- Properly cleans up temporary files on failure to prevent disk space leaks