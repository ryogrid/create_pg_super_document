# schedule_alarm

## Location
src/backend/utils/misc/timeout.c: 210 - 363

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
  - TimestampDifference
  - enable_alarm
  - setitimer
  - ITIMER_REAL
  - elog (for error reporting)
- Called from (representative examples):
  - handle_sig_alarm
  - reschedule_timeouts
  - enable_timeout_after
  - enable_timeout_every
  - enable_timeout_at
  - enable_timeouts
  - disable_timeout
  - disable_timeouts

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