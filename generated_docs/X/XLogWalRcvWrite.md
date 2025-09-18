# XLogWalRcvWrite

## Location
src/backend/replication/walreceiver.c: 910 - 992

## Overview
Writes WAL (Write-Ahead Logging) data received from the primary server to local disk storage during streaming replication, handling segment boundaries and file management.

## Definition
```c
static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli)
```

## Detailed Description
This function is responsible for the actual disk writing of WAL data received from the primary server during streaming replication. It manages WAL segment files, handles segment boundary crossings, and ensures data is properly written to the correct file offsets. The function operates in a loop to handle cases where the data spans multiple WAL segments.

Key operations include:
- Managing WAL segment file lifecycle (opening/closing files as needed)
- Calculating proper file offsets within segments 
- Writing data using pg_pwrite for atomic operations
- Updating shared memory state to reflect write progress
- Handling error conditions with appropriate panic messages

## Parameters / Member Variables
- `buf`: Buffer containing WAL data to be written to disk
- `nbytes`: Number of bytes to write from the buffer
- `recptr`: WAL record pointer indicating the LSN position for the data
- `tli`: Timeline ID associated with the WAL data

## Dependencies
- Functions called/Symbols referenced:
  - XLByteInSeg
  - XLogWalRcvClose
  - XLByteToSeg
  - XLogFileInit
  - XLogSegmentOffset
  - pg_pwrite
  - XLogFileName
  - pg_atomic_write_u64
- Called from (representative examples):
  - XLogWalRcvProcessMsg

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Uses pg_pwrite for atomic write operations to prevent partial writes
- Automatically handles WAL segment boundaries and file creation
- Updates shared memory atomically to track write progress
- Critical for data consistency during streaming replication
- Located in src/backend/replication/walreceiver.c:910-992