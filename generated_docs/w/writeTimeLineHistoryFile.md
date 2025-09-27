# writeTimeLineHistoryFile

## Location
[src/bin/pg_basebackup/receivelog.c:275-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L275-L336)

## Overview
Creates and writes a timeline history file with given content to the PostgreSQL WAL directory, using atomic file operations to ensure data integrity.

## Definition
```c
void writeTimeLineHistoryFile(TimeLineID tli, char *content, int size)
```

## Detailed Description
This function creates a timeline history file for a specified timeline ID using atomic write operations. It writes the content to a temporary file first, then atomically renames it to the final filename to avoid corruption during the write process. The function includes comprehensive error handling, proper fsync operations for durability, and wait event reporting for monitoring. This is primarily used by the walreceiver process to store timeline history information received from the primary server.

## Parameters / Member Variables
- `tli`: TimeLineID specifying which timeline this history file belongs to
- `content`: Character buffer containing the timeline history data to write
- `size`: Integer size of the content buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - XLOGDIR (macro)
  - unlink
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - write
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [pg_fsync](../p/pg_fsync.md)
  - [data_sync_elevel](../d/data_sync_elevel.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md)
  - [durable_rename](../d/durable_rename.md)
- Called from (representative examples):
  - [WalRcvFetchTimeLineHistoryFiles](../W/WalRcvFetchTimeLineHistoryFiles.md)
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md)
  - [TimeLineHistoryEntry](../T/TimeLineHistoryEntry.md)

## Notes and Other Information
- Uses atomic write pattern: temporary file creation followed by atomic rename
- Temporary files are named with process ID to avoid conflicts (/xlogtemp.PID)
- Includes comprehensive error reporting with PostgreSQL's ereport mechanism
- Performs fsync before closing to ensure data durability
- Uses wait event reporting for performance monitoring (WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE/SYNC)
- Handles disk space exhaustion by setting errno to ENOSPC when write fails
- Part of PostgreSQL's timeline management system for handling database timeline succession
- No locking required as typically called from single-threaded walreceiver context
- Replaces any existing timeline history file with the same name atomically

## Simplified Source

```c
// Simplified version of writeTimeLineHistoryFile
void writeTimeLineHistoryFile(TimeLineID tli, char *content, int size) {
    char tmppath[MAXPGPATH];
    char final_path[MAXPGPATH];
    int fd;

    // Step 1: Create temporary file path with process ID to avoid conflicts
    snprintf(tmppath, MAXPGPATH, XLOGDIR "/xlogtemp.%d", (int) getpid());
    unlink(tmppath);  // Remove any existing temp file

    // Step 2: Open temporary file for writing
    fd = OpenTransientFile(tmppath, O_RDWR | O_CREAT | O_EXCL);
    if (fd < 0) {
        ereport(ERROR, "could not create temp file");
    }

    // Step 3: Write content to temporary file
    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE);
    if (write(fd, content, size) != size) {
        unlink(tmppath);  // Clean up on failure
        ereport(ERROR, "could not write to temp file");
    }
    pgstat_report_wait_end();

    // Step 4: Ensure data is written to disk
    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC);
    if (pg_fsync(fd) != 0) {
        ereport(ERROR, "could not fsync temp file");
    }
    pgstat_report_wait_end();

    // Step 5: Close temporary file
    if (CloseTransientFile(fd) != 0) {
        ereport(ERROR, "could not close temp file");
    }

    // Step 6: Atomically move temp file to final location
    TLHistoryFilePath(final_path, tli);
    durable_rename(tmppath, final_path, ERROR);
}
```

Key simplifications made:
- Removed detailed error handling logic (save_errno, specific error messages)
- Simplified ereport calls to focus on essential error reporting
- Abstracted low-level file operations details
- Consolidated error handling patterns
- Added step-by-step comments for clarity
- Focused on the main execution path: temp file creation → write → sync → rename