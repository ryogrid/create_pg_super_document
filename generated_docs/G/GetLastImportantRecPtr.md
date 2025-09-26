# GetLastImportantRecPtr

## Location
[src/backend/access/transam/xlog.c:6535-6563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6535-L6563)

## Overview
Returns the LSN of the last important WAL record inserted, computed as the maximum across all WAL insertion locks to determine the most recent significant WAL activity.

## Definition
XLogRecPtr GetLastImportantRecPtr(void)

## Detailed Description
GetLastImportantRecPtr determines the log sequence number (LSN) of the last important WAL record that has been inserted across all WAL insertion contexts. In PostgreSQL, WAL records are classified as either important or unimportant, with all records being considered important unless explicitly marked otherwise. This distinction is crucial for checkpoint scheduling, background writer operations, and determining when significant database activity has occurred.

The function operates by iterating through all NUM_XLOGINSERT_LOCKS WAL insertion locks and finding the maximum value among their lastImportantAt fields. Each lock must be acquired exclusively to prevent torn reads of the LSN value, which could occur on some supported platforms due to the multi-byte nature of LSN values. The function returns the highest LSN found, representing the most recent important WAL activity across all concurrent insertion contexts.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - NUM_XLOGINSERT_LOCKS
  - InvalidXLogRecPtr (initial value)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - WALInsertLocks (array of insertion lock structures)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckArchiveTimeout](../C/CheckArchiveTimeout.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Used for checkpoint scheduling and background writer coordination
- Requires exclusive locking on each WAL insertion lock to prevent torn reads
- Only considers records marked as important (default for most record types)
- Critical for determining when database has been sufficiently active to warrant maintenance operations
- Returns InvalidXLogRecPtr if no important records have been inserted
- Performance scales with NUM_XLOGINSERT_LOCKS but is typically called infrequently
- Located in src/backend/access/transam/xlog.c:6535-6563