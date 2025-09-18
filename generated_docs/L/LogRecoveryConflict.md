# LogRecoveryConflict

## Location
src/backend/storage/ipc/standby.c: 273 - 358

## Overview
Logs detailed information about recovery conflicts, including wait duration, conflict type, and list of conflicting processes, for both ongoing and resolved conflicts.

## Definition
```c
void LogRecoveryConflict(ProcSignalReason reason, TimestampTz wait_start,
                        TimestampTz now, VirtualTransactionId *wait_list,
                        bool still_waiting)
```

## Detailed Description
This function provides comprehensive logging of recovery conflicts during hot standby operations. It calculates the wait duration, identifies conflicting processes by their PIDs, and generates appropriate log messages. The function handles both active conflicts (still waiting) and resolved conflicts (finished waiting), providing different message formats and detail levels for each case. When conflicting processes are present, it builds a comma-separated list of their PIDs for detailed reporting.

## Parameters / Member Variables
- `reason`: ProcSignalReason - the type of recovery conflict that occurred
- `wait_start`: TimestampTz - timestamp when the caller started waiting for conflict resolution
- `now`: TimestampTz - current timestamp when the function is called
- `wait_list`: VirtualTransactionId * - array of virtual transaction IDs of conflicting processes (NULL-terminated)
- `still_waiting`: bool - whether the startup process is still waiting or the conflict has been resolved

## Dependencies
- Functions called/Symbols referenced:
  - TimestampDifference (calculates time difference between timestamps)
  - VirtualTransactionIdIsValid (checks validity of virtual transaction ID)
  - ProcNumberGetProc (gets PGPROC structure from process number)
  - initStringInfo/appendStringInfo (builds process ID list string)
  - get_recovery_conflict_desc (gets human-readable conflict description)
  - ereport/errmsg/errdetail_log_plural (logging functions)
  - pfree (frees allocated memory)
- Called from (representative examples):
  - LockBufferForCleanup (buffer cleanup conflicts)
  - ResolveRecoveryConflictWithVirtualXIDs (virtual transaction conflicts)
  - ProcSleep (lock conflicts during recovery)

## Notes and Other Information
- Formats wait time as milliseconds with microsecond precision (e.g., "1234.567 ms")
- Builds comma-separated list of conflicting process PIDs for detailed error reporting
- Uses different log message formats for ongoing vs resolved conflicts
- Handles inactive backends gracefully (proc can be NULL)
- Uses errdetail_log_plural for proper singular/plural formatting of conflict details
- Assert ensures wait_list is NULL when conflict is resolved (still_waiting is false)
- Essential for diagnosing hot standby performance issues and conflict resolution