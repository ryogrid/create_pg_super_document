# schedule_alarm

## Location
[src/backend/utils/misc/timeout.c:210-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L210-L363)

## Overview
Schedules an alarm signal for the next active timeout using the system timer facilities.

## Definition
```c
static void schedule_alarm(TimestampTz now)
```

## Detailed Description
This internal helper function sets up the system alarm timer to trigger at the time of the next active timeout. It handles various edge cases and race conditions to ensure reliable timeout delivery. The function uses setitimer() to schedule SIGALRM delivery and includes sophisticated logic to avoid unnecessary system calls in common scenarios.

Key behaviors include:
1. Calculates time remaining until the nearest timeout
2. Handles missed signals by clearing pending flags and forcing immediate interrupts
3. Optimizes for repeated timeout set/cancel patterns by avoiding redundant setitimer() calls
4. Manages race conditions between signal scheduling and delivery
5. Ensures at least 1 microsecond delay to avoid canceling the timer

The function implements an optimization where if a signal is already pending for an earlier or equal time, it avoids making an additional setitimer() call, reducing system call overhead in high-throughput scenarios.

## Parameters / Member Variables
- `now`: Current timestamp used to calculate time remaining until next timeout

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (data type)
  - struct itimerval
  - MemSet
  - [TimestampDifference](../T/TimestampDifference.md)
  - enable_alarm
  - [setitimer](setitimer.md)
  - ITIMER_REAL
  - elog (for error reporting)
- Called from (representative examples):
  - [handle_sig_alarm](../h/handle_sig_alarm.md)
  - [reschedule_timeouts](../r/reschedule_timeouts.md)
  - [enable_timeout_after](../e/enable_timeout_after.md)
  - [enable_timeout_every](../e/enable_timeout_every.md)
  - [enable_timeout_at](../e/enable_timeout_at.md)
  - [enable_timeouts](../e/enable_timeouts.md)
  - [disable_timeout](../d/disable_timeout.md)
  - [disable_timeouts](../d/disable_timeouts.md)

## Notes and Other Information
- This is a static function internal to the timeout.c module
- Contains extensive race condition handling and optimization logic
- Uses a 10ms threshold to detect lost timeout signals
- Forces minimum 1 microsecond timer to prevent timer cancellation
- Enables the alarm handler before setting the timer to avoid race conditions
- Optimizes for high-throughput scenarios where timeouts are frequently set and canceled
- Handles cases where the current time has passed the scheduled timeout
- Will call elog(FATAL) if setitimer() system call fails
- Part of PostgreSQL's sophisticated timeout management system designed for high performance

## Simplified Source

```c
// Simplified version of schedule_alarm
static void schedule_alarm(TimestampTz now) {
    if (num_active_timeouts > 0) {
        struct itimerval timeval;
        TimestampTz nearest_timeout;
        long secs;
        int usecs;

        // Clear the timer structure
        MemSet(&timeval, 0, sizeof(struct itimerval));

        // Reset signal_pending if we think a signal was lost (>10ms overdue)
        if (signal_pending && now > signal_due_at + 10 * 1000) {
            signal_pending = false;
        }

        // Get the nearest timeout and calculate time remaining
        nearest_timeout = active_timeouts[0]->fin_time;
        if (now > nearest_timeout) {
            // We missed the timeout - schedule immediate interrupt
            signal_pending = false;
            secs = 0;
            usecs = 1;  // Force immediate interrupt
        } else {
            // Calculate time difference to nearest timeout
            TimestampDifference(now, nearest_timeout, &secs, &usecs);

            // Ensure at least 1 microsecond to avoid canceling timer
            if (secs == 0 && usecs == 0) {
                usecs = 1;
            }
        }

        // Set up the timer values
        timeval.it_value.tv_sec = secs;
        timeval.it_value.tv_usec = usecs;

        // Enable alarm handler before setting timer (race condition prevention)
        enable_alarm();

        // Skip setitimer if we already have a pending signal for earlier/same time
        if (signal_pending && nearest_timeout >= signal_due_at) {
            return;
        }

        // Mark signal as pending and record when it's due
        signal_due_at = nearest_timeout;
        signal_pending = true;

        // Set the system alarm timer
        if (setitimer(ITIMER_REAL, &timeval, NULL) != 0) {
            signal_pending = false;
            elog(FATAL, "could not enable SIGALRM timer: %m");
        }
    }
}
```

Key simplifications made:
- Removed extensive comments about race conditions while preserving the actual race condition handling logic
- Consolidated the logic flow into clearer sections: signal loss detection, timeout calculation, timer setup, and system call
- Simplified variable declarations and kept essential error handling
- Preserved the core optimization that avoids redundant setitimer() calls
- Maintained all critical timing and synchronization logic