# win32_timer_callback

## Location
[src/bin/pgbench/pgbench.c:7762-7767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7762-L7767)

## Overview
A Windows-specific timer callback function used in pgbench to signal when the specified time duration has been exceeded during benchmark execution.

## Definition

```c
static VOID CALLBACK
win32_timer_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
```
## Detailed Description
This function serves as a Windows timer callback that is executed by the Windows Timer Queue API when a timer expires. It is the Windows equivalent of the Unix signal handler  used for implementing pgbench's duration-based testing functionality. When called, it sets the global  flag to , which causes the benchmark execution loop to terminate gracefully.

The function is designed to work with Windows Timer Queue timers created via  and is executed in a timer thread context. It provides a platform-specific implementation for duration-limited benchmark runs in pgbench.

## Parameters / Member Variables
- `lpParameter`: A user-defined parameter passed to the timer callback (unused in this implementation, set to NULL)
- `TimerOrWaitFired`: A boolean indicating whether the callback was called because the timer fired (TRUE) or because an associated wait object was signaled (FALSE)
## Dependencies
- Functions called/Symbols referenced:
  - timer_exceeded (global volatile variable)
- Called from (representative examples):
  - Used as callback in CreateTimerQueueTimer() in setalarm() function at src/bin/pgbench/pgbench.c:7777

## Notes and Other Information
- This function is only compiled on Windows platforms (within  block)
- The function uses the Windows CALLBACK calling convention
- It is part of pgbench's cross-platform timer implementation for duration-based benchmark testing
- The corresponding Unix implementation uses SIGALRM signal handling with  function
- The  flag is checked throughout the pgbench execution loop to determine when to stop the benchmark
- This callback is executed in a separate timer thread, making the  qualifier on  necessary for thread safety

## Simplified Source

```c
static VOID CALLBACK
win32_timer_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
{
    // Signal that the benchmark duration has been exceeded
    timer_exceeded = true;
}
```