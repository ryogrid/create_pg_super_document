# setitimer

## Location
[src/backend/port/win32/timer.c:86-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/timer.c#L86-L121)

## Overview
A Windows emulation of the POSIX setitimer() function that creates and manages timer threads for alarm signal generation.

## Definition


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
  - schedule_alarm (timeout.c:339)
  - [fork_process](../f/fork_process.md) (fork_process.c:72)
  - [do_watch](../d/do_watch.md) (psql command.c:5384, 5560)

## Notes and Other Information
- Windows-specific implementation for systems lacking native setitimer()
- Thread creation occurs only on first call per backend process
- Uses critical sections for thread-safe timer value updates
- Fatal errors reported if Windows API calls fail during initialization
- Converts POSIX timer semantics to Windows event-driven model
- Limited to real-time timers only (no virtual or profiling timers)