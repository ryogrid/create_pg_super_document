# xact_redo

## Location
[src/backend/access/transam/xact.c:6301-6384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L6301-L6384)

## Overview
Main entry point for replaying transaction-related WAL records during PostgreSQL recovery, dispatching to appropriate handler functions based on record type.

## Definition

```c
void
xact_redo(XLogReaderState *record)
```
## Detailed Description
This function serves as the central dispatcher for all transaction-related WAL record replay during crash recovery and hot standby. It examines the operation code from the WAL record and routes to the appropriate specialized replay function. The function handles six different types of transaction records: regular commits, prepared transaction commits, regular aborts, prepared transaction aborts, transaction preparations, and transaction assignments. For two-phase commit operations, it also manages the cleanup of TwoPhaseState entries and associated files.

## Parameters / Member Variables
- : XLogReaderState structure containing the WAL record to be replayed, including record data, transaction ID, LSN, and origin information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecHasAnyBlockRefs
  - XLogRecGetData
  - XLogRecGetXid
  - XLogRecGetOrigin
  - [ParseCommitRecord](../P/ParseCommitRecord.md)
  - [ParseAbortRecord](../P/ParseAbortRecord.md)
  - [xact_redo_commit](xact_redo_commit.md)
  - [xact_redo_abort](xact_redo_abort.md)
  - [PrepareRedoAdd](../P/PrepareRedoAdd.md)
  - [PrepareRedoRemove](../P/PrepareRedoRemove.md)
  - ProcArrayApplyXidAssignment
  - LWLockAcquire
  - LWLockRelease
- Called from (representative examples):
  - WAL recovery system (registered as resource manager redo function)

## Notes and Other Information
The function includes assertions to verify that transaction records don't contain backup blocks, which is expected for transaction records. For XLOG_XACT_INVALIDATIONS records, the function currently ignores them as invalidations are processed through commit records. Two-phase commit operations require additional coordination with TwoPhaseStateLock to manage global transaction state. The function uses elog(PANIC) for unknown operation codes to ensure system integrity during recovery. Transaction assignment records are only processed when the standby state is at least STANDBY_INITIALIZED to ensure proper hot standby functionality.