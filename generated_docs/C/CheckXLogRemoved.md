# CheckXLogRemoved

## Location
[src/backend/access/transam/xlog.c:3704-3734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3704-L3734)

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
  - [XLogFileName](../X/XLogFileName.md): Constructs WAL filename for error reporting
  - ereport: Reports errors with appropriate error codes and messages
  - [errcode_for_file_access](../e/errcode_for_file_access.md): Provides appropriate error code for file access issues
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md): Multiple locations during base backup operations
  - [logical_read_xlog_page](../l/logical_read_xlog_page.md): During logical replication WAL reading
  - [XLogSendPhysical](../X/XLogSendPhysical.md): During physical WAL replication
  - [WALAvailability](../W/WALAvailability.md): For checking WAL segment availability status

## Notes and Other Information
- The function guarantees errno preservation, making it safe to use in error handling paths
- Accesses XLogCtl->lastRemovedSegNo under spin lock protection for thread safety
- Always succeeds if no WAL segments have been removed since server startup
- Designed specifically for segments known to have existed during server runtime
- Error messages include the complete WAL filename for diagnostic purposes
- Critical for maintaining data consistency in backup and replication scenarios

## Simplified Source

```c
// Simplified version of CheckXLogRemoved
void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli) {
    int save_errno = errno;
    XLogSegNo lastRemovedSegNo;

    // Get last removed segment number under lock
    SpinLockAcquire(&XLogCtl->info_lck);
    lastRemovedSegNo = XLogCtl->lastRemovedSegNo;
    SpinLockRelease(&XLogCtl->info_lck);

    // Check if requested segment has been removed
    if (segno <= lastRemovedSegNo) {
        char filename[MAXFNAMELEN];

        XLogFileName(filename, tli, segno, wal_segment_size);
        errno = save_errno;
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("requested WAL segment %s has already been removed",
                        filename)));
    }

    errno = save_errno;
}
```

Key simplifications made:
- Removed detailed comments while preserving core logic
- Maintained errno preservation for safe error handling
- Preserved thread-safe access to shared data with spin locks
- Kept comprehensive error reporting with filename context
- Maintained the essential WAL segment availability check