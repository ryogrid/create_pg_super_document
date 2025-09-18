# CheckXLogRemoved

## Location
src/backend/access/transam/xlog.c: 3704 - 3734

## Overview
CheckXLogRemoved verifies that a specified WAL segment has not been removed or recycled, throwing an error if the segment is no longer available.

## Definition
```c
void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli)
```

## Detailed Description
This function provides a crucial validation mechanism for WAL segment availability by comparing a requested segment number against the last known removed segment. It serves as a safety check to prevent attempts to access WAL segments that have already been removed from the system through normal maintenance operations like checkpointing or archiving. The function is designed to be used with segments that are known to have existed while the server was running, as it will always succeed if no WAL segments have been removed since startup.

The implementation carefully preserves the errno value throughout execution to support callers that may want to provide enhanced error messages while still maintaining normal file-access error handling. When a removed segment is detected, the function generates a descriptive error message including the specific WAL filename that was requested.

## Parameters / Member Variables
- `segno`: XLogSegNo representing the WAL segment number to check for availability
- `tli`: TimeLineID used only in error message formatting to provide context about which timeline the segment belongs to

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire: Acquires spin lock for thread-safe access to shared data
  - SpinLockRelease: Releases spin lock after accessing shared data
  - XLogFileName: Constructs WAL filename for error reporting
  - ereport: Reports errors with appropriate error codes and messages
  - errcode_for_file_access: Provides appropriate error code for file access issues
- Called from (representative examples):
  - perform_base_backup: Multiple locations during base backup operations
  - logical_read_xlog_page: During logical replication WAL reading
  - XLogSendPhysical: During physical WAL replication
  - WALAvailability: For checking WAL segment availability status

## Notes and Other Information
- The function guarantees errno preservation, making it safe to use in error handling paths
- Accesses XLogCtl->lastRemovedSegNo under spin lock protection for thread safety
- Always succeeds if no WAL segments have been removed since server startup
- Designed specifically for segments known to have existed during server runtime
- Error messages include the complete WAL filename for diagnostic purposes
- Critical for maintaining data consistency in backup and replication scenarios