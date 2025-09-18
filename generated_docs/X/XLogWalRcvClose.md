# XLogWalRcvClose

## Location
[src/backend/replication/walreceiver.c:1048-1099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1048-L1099)

## Overview
Closes the currently open WAL segment file after ensuring data persistence through flushing and sets up appropriate archive notifications for the completed segment.

## Definition
```c
static void XLogWalRcvClose(XLogRecPtr recptr, TimeLineID tli)
```

## Detailed Description
This function handles the proper closure of a WAL segment file during streaming replication. It performs several critical operations to ensure data integrity and proper archival workflow:

1. **Data Persistence**: Calls XLogWalRcvFlush to ensure all data is synchronized to disk before closing
2. **File Closure**: Safely closes the file descriptor with error handling
3. **Archive Notification**: Creates appropriate archive notification files (.done or .ready) depending on the archive mode configuration
4. **State Cleanup**: Resets the receive file descriptor to indicate no file is currently open

The function distinguishes between different archive modes - for non-ALWAYS modes it creates a .done file to prevent re-archival, while for ALWAYS mode it creates a .ready file for normal archival processing.

## Parameters / Member Variables
- `recptr`: WAL record pointer indicating the current position (used for validation that we're at a segment boundary)
- `tli`: Timeline ID associated with the segment being closed

## Dependencies
- Functions called/Symbols referenced:
  - XLByteInSeg
  - [XLogWalRcvFlush](XLogWalRcvFlush.md)
  - [XLogFileName](XLogFileName.md)
  - close
  - [XLogArchiveForceDone](XLogArchiveForceDone.md)
  - [XLogArchiveNotify](XLogArchiveNotify.md)
- Called from (representative examples):
  - [XLogWalRcvWrite](XLogWalRcvWrite.md)

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Always flushes data before closing to avoid need for later fsync operations
- Does not advise OS to release cache pages (unlike XLogFileClose) since recovery will re-read the files soon
- Handles different archive modes appropriately for streaming replication scenarios
- Includes assertions to validate segment boundary conditions
- Located in src/backend/replication/walreceiver.c:1048-1099