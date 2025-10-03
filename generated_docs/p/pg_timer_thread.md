# pg_timer_thread

## Location
[src/backend/port/win32/timer.c:36-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/timer.c#L36-L85)

## Overview
A Windows-specific timer management thread function that handles timer events and signals SIGALRM when timeouts expire.

## Definition

```c
static DWORD WINAPI
pg_timer_thread(LPVOID param)
```
## Detailed Description
The  function implements the core timer thread for PostgreSQL's Windows port timer emulation. It runs in an infinite loop, waiting for timer events or timeout expiration. When the main thread signals a timer change via the communication area event, this thread updates its wait time accordingly. When a timeout occurs, it signals SIGALRM to the main process and resets to infinite wait time. This provides POSIX-style timer functionality on Windows systems that lack native setitimer() support.

## Parameters / Member Variables
- `param`: Thread parameter (unused, expected to be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - WaitForSingleObjectEx (Windows API)
  - EnterCriticalSection/LeaveCriticalSection (Windows API)
  - ResetEvent/SetEvent (Windows API)
  - [pg_queue_signal](pg_queue_signal.md)
  - SIGALRM
  - timerCommArea (global communication structure)
- Called from (representative examples):
  - [setitimer](../s/setitimer.md) (via CreateThread)

## Notes and Other Information
- Windows-specific implementation using Win32 API
- Thread runs with infinite loop until process termination
- Uses millisecond precision for timeouts (microseconds rounded up)
- Critical section synchronization prevents race conditions with main thread
- Only supports one-shot timers (no interval timers)
- Timeout calculations convert microseconds to milliseconds with rounding up

## Simplified Source

```c
// Simplified version of pg_timer_thread
static DWORD WINAPI
pg_timer_thread(LPVOID param)
{
    DWORD waittime = INFINITE;

    // Main timer thread loop
    for (;;)
    {
        // Wait for timer event or timeout
        int result = WaitForSingleObjectEx(timerCommArea.event, waittime, FALSE);

        if (result == WAIT_OBJECT_0)
        {
            // Timer configuration changed by main thread
            EnterCriticalSection(&timerCommArea.crit_sec);

            if (timer_is_cancelled())
                waittime = INFINITE;  // No timeout needed
            else
                waittime = convert_to_milliseconds(timerCommArea.value);

            ResetEvent(timerCommArea.event);
            LeaveCriticalSection(&timerCommArea.crit_sec);
        }
        else if (result == WAIT_TIMEOUT)
        {
            // Timer expired - signal alarm and reset
            pg_queue_signal(SIGALRM);
            waittime = INFINITE;
        }
        // Note: Error cases removed for clarity
    }

    return 0;
}
```

Key simplifications made:
- Abstracted timer cancellation check into `timer_is_cancelled()` helper concept
- Simplified timeout calculation into `convert_to_milliseconds()` concept
- Removed detailed error handling and assertions for clarity
- Added descriptive comments for main logic flow
- Consolidated the timer value checking logic
- Focused on the core timer management algorithm