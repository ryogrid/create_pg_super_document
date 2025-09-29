# ConditionVariableTimedSleep

## Location
[src/backend/storage/lmgr/condition_variable.c:112-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/condition_variable.c#L112-L229)

## Overview
Waits for a condition variable to be signaled or for a specified timeout to be reached, providing the fundamental timed blocking mechanism for condition variable synchronization.

## Definition
```c
bool ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 wait_event_info)
```

## Detailed Description
ConditionVariableTimedSleep is the core implementation function for condition variable waiting with timeout support. It handles both indefinite waiting (when timeout is -1) and timed waiting (when timeout is a positive value in milliseconds). The function returns true if the timeout expires, false otherwise.

The function manages several critical aspects of condition variable behavior:
1. Automatically prepares for sleep if not already prepared, returning immediately to allow the caller's predicate loop to re-test the exit condition
2. Uses WaitLatch as the underlying blocking mechanism, waiting for the process latch to be set
3. Implements spurious wakeup handling by checking if the process was actually removed from the wait list
4. Maintains accurate timeout calculation across multiple wake-up cycles
5. Handles interrupts and ensures the sleep target hasn't changed during interrupt processing

The implementation uses a loop to handle spurious wakeups, only returning when either signaled properly, timed out, or interrupted.

## Parameters / Member Variables
- `cv`: Pointer to the ConditionVariable to wait on
- `timeout`: Timeout in milliseconds (-1 for no timeout, >=0 for timed wait)
- `wait_event_info`: Value from WaitEventXXX enums for monitoring in pg_stat_activity

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionVariablePrepareToSleep](ConditionVariablePrepareToSleep.md) (prepares sleep if not already prepared)
  - [WaitLatch](../W/WaitLatch.md) (underlying blocking mechanism using latch system)
  - [ResetLatch](../R/ResetLatch.md) (clears the latch after waking up)
  - proclist_contains (checks if process is still in wait list)
  - proclist_push_tail (re-adds process to wait list after signaling)
  - INSTR_TIME_* macros (time measurement for timeout calculation)
  - CHECK_FOR_INTERRUPTS (handles interrupts during wait)
- Called from (representative examples):
  - [recoveryPausesHere](../r/recoveryPausesHere.md)
  - [RecoveryRequiresIntParameter](../R/RecoveryRequiresIntParameter.md)
  - [WaitForWalSummarization](../W/WaitForWalSummarization.md)
  - [WaitForStandbyConfirmation](../W/WaitForStandbyConfirmation.md)
  - [WaitForProcSignalBarrier](../W/WaitForProcSignalBarrier.md)
  - [ConditionVariableSleep](ConditionVariableSleep.md) (as the underlying implementation)

## Notes and Other Information
- Returns true on timeout, false when signaled or interrupted
- Automatically handles case where caller didn't call ConditionVariablePrepareToSleep
- Implements spurious wakeup protection by re-checking wait list membership
- Maintains process in wait list after being signaled to avoid missing subsequent signals
- Uses high-precision timing to maintain accurate timeout across multiple wakeups
- Supports both WL_TIMEOUT and non-timeout wait event sets
- Critical for timeout-based operations in recovery, replication, and inter-process signaling
- The underlying implementation for all condition variable sleep operations

## Simplified Source
```c
bool ConditionVariableTimedSleep(ConditionVariable *cv, long timeout,
                                uint32 wait_event_info)
{
    long cur_timeout = -1;
    instr_time start_time;
    instr_time cur_time;
    int wait_events;

    // If not prepared to sleep on this CV, prepare and return immediately
    if (cv_sleep_target != cv) {
        ConditionVariablePrepareToSleep(cv);
        return false;
    }

    // Set up timeout tracking if needed
    if (timeout >= 0) {
        INSTR_TIME_SET_CURRENT(start_time);
        cur_timeout = timeout;
        wait_events = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH;
    } else {
        wait_events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
    }

    while (true) {
        bool done = false;

        // Wait for latch to be set
        (void) WaitLatch(MyLatch, wait_events, cur_timeout, wait_event_info);
        ResetLatch(MyLatch);

        // Check if we were properly signaled (removed from wait list)
        SpinLockAcquire(&cv->mutex);
        if (!proclist_contains(&cv->wakeup, MyProcNumber, cvWaitLink)) {
            done = true;
            // Re-add ourselves to wait list for future signals
            proclist_push_tail(&cv->wakeup, MyProcNumber, cvWaitLink);
        }
        SpinLockRelease(&cv->mutex);

        // Handle interrupts and check if sleep target changed
        CHECK_FOR_INTERRUPTS();
        if (cv != cv_sleep_target)
            done = true;

        if (done)
            return false;  // We were signaled

        // Update timeout for next iteration
        if (timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(cur_time);

            // Check if timeout expired
            if (cur_timeout <= 0)
                return true;  // Timeout reached
        }
    }
}
```

This function implements timed condition variable waiting by:
1. Preparing for sleep if not already set up for this condition variable
2. Setting up timeout tracking and appropriate wait events
3. Looping until signaled, interrupted, or timed out
4. Using the latch system for actual blocking
5. Distinguishing between proper signals and spurious wakeups
6. Maintaining accurate timeout calculation across multiple wakeup cycles