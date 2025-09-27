# setitimer

## Location
[src/backend/port/win32/timer.c:86-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/timer.c#L86-L121)

## Overview
A Windows emulation of the POSIX setitimer() function that creates and manages timer threads for alarm signal generation.

## Definition

```c
int
setitimer(int which, const struct itimerval *value, struct itimerval *ovalue)
```
## Detailed Description
The `setitimer` function provides POSIX setitimer() compatibility on Windows by emulating timer functionality through a dedicated thread. On first call, it initializes the timer communication area, creates a Windows event object, and spawns the timer management thread. Subsequent calls update the timer value through thread-safe communication. The function only supports ITIMER_REAL timers and one-shot timeouts (no interval timers). This implementation bridges the gap between POSIX timer semantics and Windows threading/event model.

## Parameters / Member Variables
- `which`: Timer type (must be ITIMER_REAL)
- `value`: Pointer to new timer value structure containing timeout specification
- `ovalue`: Pointer to receive previous timer value (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - CreateEvent/SetEvent (Windows API)
  - CreateThread (Windows API)
  - InitializeCriticalSection/EnterCriticalSection/LeaveCriticalSection (Windows API)
  - MemSet
  - ereport/errmsg_internal
  - [pg_timer_thread](../p/pg_timer_thread.md)
  - ITIMER_REAL
  - [itimerval](../i/itimerval.md)
  - timerCommArea (global communication structure)
- Called from (representative examples):
  - [schedule_alarm](schedule_alarm.md) (timeout.c:339)
  - [fork_process](../f/fork_process.md) (fork_process.c:72)
  - [do_watch](../d/do_watch.md) (psql command.c:5384, 5560)

## Notes and Other Information
- Windows-specific implementation for systems lacking native setitimer()
- Thread creation occurs only on first call per backend process
- Uses critical sections for thread-safe timer value updates
- Fatal errors reported if Windows API calls fail during initialization
- Converts POSIX timer semantics to Windows event-driven model
- Limited to real-time timers only (no virtual or profiling timers)

## Simplified Source

```c
// Simplified version of setitimer (Windows implementation)
int setitimer(int which, const struct itimerval *value, struct itimerval *ovalue) {
    // Validate parameters
    Assert(value != NULL);
    Assert(value->it_interval.tv_sec == 0 && value->it_interval.tv_usec == 0);
    Assert(which == ITIMER_REAL);

    // Initialize timer thread on first call
    if (timerThreadHandle == INVALID_HANDLE_VALUE) {
        // Create Windows event for thread communication
        timerCommArea.event = CreateEvent(NULL, TRUE, FALSE, NULL);
        if (timerCommArea.event == NULL) {
            ereport(FATAL, (errmsg_internal("could not create timer event: error code %lu",
                                           GetLastError())));
        }

        // Initialize timer communication area
        MemSet(&timerCommArea.value, 0, sizeof(struct itimerval));
        InitializeCriticalSection(&timerCommArea.crit_sec);

        // Create timer management thread
        timerThreadHandle = CreateThread(NULL, 0, pg_timer_thread, NULL, 0, NULL);
        if (timerThreadHandle == INVALID_HANDLE_VALUE) {
            ereport(FATAL, (errmsg_internal("could not create timer thread: error code %lu",
                                           GetLastError())));
        }
    }

    // Update timer settings thread-safely
    EnterCriticalSection(&timerCommArea.crit_sec);

    // Return old value if requested
    if (ovalue) {
        *ovalue = timerCommArea.value;
    }

    // Set new timer value
    timerCommArea.value = *value;

    LeaveCriticalSection(&timerCommArea.crit_sec);

    // Signal timer thread to update settings
    SetEvent(timerCommArea.event);

    return 0;
}
```

Key simplifications made:
- Added clear comments for the initialization and update phases
- Preserved the essential Windows API calls and error handling
- Maintained the thread-safe communication mechanism
- Kept the important parameter validation logic