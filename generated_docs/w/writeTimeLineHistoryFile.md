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
  - OpenTransientFile
  - pgstat_report_wait_start
  - write
  - pgstat_report_wait_end
  - pg_fsync
  - data_sync_elevel
  - CloseTransientFile
  - TLHistoryFilePath
  - [durable_rename](../d/durable_rename.md)
- Called from (representative examples):
  - [WalRcvFetchTimeLineHistoryFiles](../W/WalRcvFetchTimeLineHistoryFiles.md)
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md)
  - TimeLineHistoryEntry

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