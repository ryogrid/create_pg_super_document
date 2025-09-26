# WaitForWalSummarization

## Location
[src/backend/postmaster/walsummarizer.c:660-787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L660-L787)

## Overview
Waits until WAL summarization reaches a specified LSN with timeout and error handling, providing a mechanism for processes to ensure WAL summarization has progressed to a required point.

## Definition

```c
void
WaitForWalSummarization(XLogRecPtr lsn)
```
## Detailed Description
This function blocks until the WAL summarizer has processed and summarized WAL records up to the specified LSN. It implements a polling mechanism with timeout cycles, progress monitoring, and intelligent error reporting. The function includes safeguards against hanging indefinitely by detecting when the summarizer is not making progress and reporting detailed status information.

The function monitors both the summarized_lsn (data written to disk) and pending_lsn (data processed in memory) to distinguish between different types of stalls. It provides progressively more detailed error messages if the summarizer appears to be stuck, ultimately throwing an error if no progress is detected for 60 seconds (6 cycles of 10 seconds each).

## Parameters / Member Variables
- : The target XLogRecPtr (LSN) that WAL summarization must reach before the function returns

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - TimestampTzPlusMilliseconds
  - [ConditionVariableTimedSleep](../C/ConditionVariableTimedSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg_plural](../e/errmsg_plural.md)
  - [errdetail](../e/errdetail.md)
  - LSN_FORMAT_ARGS
- Global variables accessed:
  - summarize_wal
  - WalSummarizerCtl
- Called from (representative examples):
  - [CleanupAfterArchiveRecovery](../C/CleanupAfterArchiveRecovery.md)
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)

## Notes and Other Information
- Returns immediately if summarize_wal is disabled during the wait
- Uses 10-second timeout cycles for polling with aligned timing to prevent drift
- Monitors both disk-persisted (summarized_lsn) and in-memory (pending_lsn) progress
- Provides detailed error messages showing target, disk, and memory LSN positions
- Throws ERROR after 60 seconds of no progress (deadcycles >= 6)
- Issues WARNING messages every 10 seconds to keep users informed of wait status
- Uses condition variables for efficient sleeping between checks
- Designed to handle the case where the summarizer process may not be running