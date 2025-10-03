# DetermineSleepTime

## Location
[src/backend/postmaster/postmaster.c:1518-1602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1518-L1602)

## Overview
Calculates the optimal sleep duration in milliseconds for the PostgreSQL postmaster's ServerLoop to balance responsiveness with resource efficiency.

## Definition

```c
static int
DetermineSleepTime(void)
```
## Detailed Description
DetermineSleepTime is a critical function in the postmaster's event loop that determines how long the server should wait before checking for new events. The function implements intelligent sleep duration calculation based on the current server state:

- **Normal operation**: Returns 60 seconds (60,000ms) to allow periodic maintenance tasks
- **Shutdown sequence**: Calculates remaining time before SIGKILL based on AbortStartTime
- **Background worker startup needed**: Returns 0 for immediate processing
- **Crashed background workers**: Calculates minimum time until next restart attempt

The function ensures that background workers are serviced promptly while maintaining efficient resource usage during normal operations. During shutdown, it provides a countdown mechanism for graceful termination before forceful process killing.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - TimestampTzPlusMilliseconds
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)  
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md)
  - slist_foreach_modify
  - slist_container
- Constants used:
  - NoShutdown
  - SIGKILL_CHILDREN_AFTER_SECS
  - BGW_NEVER_RESTART
- Called from:
  - [ServerLoop](../S/ServerLoop.md)

## Notes and Other Information
- The function uses global variables like Shutdown, AbortStartTime, StartWorkerNeeded, and HaveCrashedWorker to determine server state
- Background worker restart timing is calculated using the worker's bgw_restart_time and crash timestamp
- The maximum sleep time is capped at 60 seconds to ensure periodic maintenance tasks execute
- During shutdown, the sleep time decreases as the SIGKILL deadline approaches
- Workers marked as BGW_NEVER_RESTART or flagged for termination are removed from the background worker list

## Simplified Source

```c
// Simplified version of DetermineSleepTime
static int DetermineSleepTime(void) {
    TimestampTz next_wakeup = 0;

    // Normal case: no background workers need attention or we're shutting down
    if (Shutdown > NoShutdown || (!StartWorkerNeeded && !HaveCrashedWorker)) {
        // If we're in abort sequence, calculate time left before SIGKILL
        if (AbortStartTime != 0) {
            int seconds_left = SIGKILL_CHILDREN_AFTER_SECS - (time(NULL) - AbortStartTime);
            return Max(seconds_left * 1000, 0);  // Convert to milliseconds, clamp to 0
        }
        // Normal operation: sleep for one minute
        return 60 * 1000;
    }

    // Background worker needs immediate startup
    if (StartWorkerNeeded) {
        return 0;  // No sleep, process immediately
    }

    // Handle crashed background workers that need restart
    if (HaveCrashedWorker) {
        // Find the earliest restart time among all crashed workers
        for each worker in BackgroundWorkerList {
            if (worker not crashed)
                continue;

            if (worker should never restart || worker terminating) {
                remove worker from list;
                continue;
            }

            // Calculate when this worker should be restarted
            TimestampTz restart_time = worker.crashed_at + (worker.restart_interval * 1000);

            // Track the earliest restart time
            if (next_wakeup == 0 || restart_time < next_wakeup) {
                next_wakeup = restart_time;
            }
        }
    }

    // If we have a specific wakeup time, calculate milliseconds until then
    if (next_wakeup != 0) {
        int ms_until_wakeup = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), next_wakeup);
        return Min(60 * 1000, ms_until_wakeup);  // Cap at 60 seconds
    }

    // Default: sleep for one minute
    return 60 * 1000;
}
```

Key simplifications made:
- Replaced complex loop with simplified pseudocode for clarity
- Removed low-level list manipulation details (slist_foreach_modify, slist_container)
- Consolidated similar conditional branches
- Added descriptive comments for each major logic section
- Simplified variable names and calculations where possible
- Focused on the main decision flow rather than implementation details