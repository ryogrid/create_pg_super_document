# DeadLockReport

## Location
[src/backend/storage/lmgr/deadlock.c:1072-1143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L1072-L1143)

## Overview
Reports a detected deadlock with detailed information for both client and server log, including process IDs, lock types, and query details.

## Definition
```c
void DeadLockReport(void)
```

## Detailed Description
DeadLockReport is responsible for formatting and reporting deadlock information when a deadlock is detected in PostgreSQL. The function constructs detailed error messages that include:

1. **Client-facing information**: A formatted list of processes and the locks they are waiting for, showing the circular dependency that forms the deadlock
2. **Server log information**: Extended details including the current query strings being executed by each process involved in the deadlock

The function iterates through the global `deadlockDetails` array to build comprehensive error messages. It uses `StringInfo` buffers to construct the messages and ultimately calls `ereport(ERROR, ...)` to report the deadlock as a PostgreSQL error with error code `ERRCODE_T_R_DEADLOCK_DETECTED`.

## Parameters / Member Variables
This function takes no parameters but operates on global state:
- Uses global `deadlockDetails` array containing information about processes involved in the deadlock
- Uses global `nDeadlockDetails` indicating the number of processes in the deadlock cycle

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [resetStringInfo](../r/resetStringInfo.md)  
  - [DescribeLockTag](DescribeLockTag.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [GetLockmodeName](../G/GetLockmodeName.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [pgstat_get_backend_current_activity](../p/pgstat_get_backend_current_activity.md)
  - [pgstat_report_deadlock](../p/pgstat_report_deadlock.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [errdetail_log](../e/errdetail_log.md)
  - [errhint](../e/errhint.md)
  - DEADLOCK_INFO (struct type)
  - ERRCODE_T_R_DEADLOCK_DETECTED

- Called from (representative examples):
  - [WaitOnLock](../W/WaitOnLock.md) (src/backend/storage/lmgr/lock.c:1872)

## Notes and Other Information
- The function generates two separate message buffers: one sanitized for client consumption and another with full query details for server logs
- The error reporting includes a hint directing users to check the server log for complete query details
- This function is part of PostgreSQL's deadlock detection and resolution mechanism
- The function calls `pgstat_report_deadlock()` to update statistics about deadlock occurrences
- The function assumes that deadlock detection has already been performed and the `deadlockDetails` global array has been populated