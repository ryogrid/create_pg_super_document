# WaitForWalSummarization

## Location
src/backend/postmaster/walsummarizer.c: 660 - 787

## Overview
Waits until WAL summarization reaches a specified LSN with timeout and error handling, providing a mechanism for processes to ensure WAL summarization has progressed to a required point.

## Definition


## Detailed Description
This function blocks until the WAL summarizer has processed and summarized WAL records up to the specified LSN. It implements a polling mechanism with timeout cycles, progress monitoring, and intelligent error reporting. The function includes safeguards against hanging indefinitely by detecting when the summarizer is not making progress and reporting detailed status information.

The function monitors both the summarized_lsn (data written to disk) and pending_lsn (data processed in memory) to distinguish between different types of stalls. It provides progressively more detailed error messages if the summarizer appears to be stuck, ultimately throwing an error if no progress is detected for 60 seconds (6 cycles of 10 seconds each).

## Parameters / Member Variables
- : The target XLogRecPtr (LSN) that WAL summarization must reach before the function returns

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - LWLockAcquire
  - LWLockRelease
  - GetCurrentTimestamp
  - TimestampDifferenceMilliseconds
  - TimestampTzPlusMilliseconds
  - ConditionVariableTimedSleep
  - ConditionVariableCancelSleep
  - ereport
  - errcode
  - errmsg_plural
  - errdetail
  - LSN_FORMAT_ARGS
- Global variables accessed:
  - summarize_wal
  - WalSummarizerCtl
- Called from (representative examples):
  - CleanupAfterArchiveRecovery
  - PrepareForIncrementalBackup

## Notes and Other Information
- Returns immediately if summarize_wal is disabled during the wait
- Uses 10-second timeout cycles for polling with aligned timing to prevent drift
- Monitors both disk-persisted (summarized_lsn) and in-memory (pending_lsn) progress
- Provides detailed error messages showing target, disk, and memory LSN positions
- Throws ERROR after 60 seconds of no progress (deadcycles >= 6)
- Issues WARNING messages every 10 seconds to keep users informed of wait status
- Uses condition variables for efficient sleeping between checks
- Designed to handle the case where the summarizer process may not be running