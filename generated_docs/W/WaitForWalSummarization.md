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

## Simplified Source

```c
// Simplified version of WaitForWalSummarization
void WaitForWalSummarization(XLogRecPtr lsn) {
    TimestampTz initial_time, cycle_time, current_time;
    XLogRecPtr prior_pending_lsn = InvalidXLogRecPtr;
    int deadcycles = 0;

    initial_time = cycle_time = GetCurrentTimestamp();

    while (1) {
        long timeout_in_ms = 10000;  // 10-second cycles
        XLogRecPtr summarized_lsn, pending_lsn;

        CHECK_FOR_INTERRUPTS();

        // Exit if WAL summarization is disabled
        if (!summarize_wal)
            return;

        // Get current summarization progress (with lock protection)
        LWLockAcquire(WALSummarizerLock, LW_SHARED);
        summarized_lsn = WalSummarizerCtl->summarized_lsn;
        pending_lsn = WalSummarizerCtl->pending_lsn;
        LWLockRelease(WALSummarizerLock);

        // Success: summarization has reached target LSN
        if (summarized_lsn >= lsn)
            break;

        current_time = GetCurrentTimestamp();

        // Check if current 10-second cycle is complete
        if (TimestampDifferenceMilliseconds(cycle_time, current_time) >= timeout_in_ms) {
            // Start new cycle
            cycle_time = TimestampTzPlusMilliseconds(cycle_time, timeout_in_ms);

            // Track progress: has pending_lsn advanced?
            if (pending_lsn > prior_pending_lsn) {
                prior_pending_lsn = pending_lsn;
                deadcycles = 0;  // Reset stall counter
            } else {
                ++deadcycles;    // Another cycle with no progress
            }

            // Error after 60 seconds (6 cycles) of no progress
            if (deadcycles >= 6) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("WAL summarization is not progressing"),
                     errdetail("Stuck at %X/%X on disk, %X/%X in memory, need %X/%X",
                              LSN_FORMAT_ARGS(summarized_lsn),
                              LSN_FORMAT_ARGS(pending_lsn),
                              LSN_FORMAT_ARGS(lsn))));
            }

            // Warn user about ongoing wait
            long elapsed_seconds = TimestampDifferenceMilliseconds(initial_time, current_time) / 1000;
            ereport(WARNING,
                (errmsg("still waiting for WAL summarization through %X/%X after %ld seconds",
                        LSN_FORMAT_ARGS(lsn), elapsed_seconds),
                 errdetail("Reached %X/%X on disk, %X/%X in memory",
                          LSN_FORMAT_ARGS(summarized_lsn),
                          LSN_FORMAT_ARGS(pending_lsn))));
        }

        // Adjust timeout to maintain cycle alignment
        timeout_in_ms -= TimestampDifferenceMilliseconds(cycle_time, current_time);

        // Sleep until next check (or timeout)
        ConditionVariableTimedSleep(&WalSummarizerCtl->summary_file_cv,
                                   timeout_in_ms,
                                   WAIT_EVENT_WAL_SUMMARY_READY);
    }

    ConditionVariableCancelSleep();
}
```

Key simplifications made:
- Consolidated timestamp management logic for better readability
- Simplified error message formatting while preserving essential information
- Added inline comments explaining the purpose of each major section
- Removed verbose comment blocks but preserved the core algorithm
- Streamlined the progress tracking logic with clearer variable names
- Condensed the detailed error reporting while maintaining key diagnostic information
- Preserved all critical functionality: timeout cycles, progress monitoring, error detection