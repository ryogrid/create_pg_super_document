# setalarm

## Location
[src/bin/pgbench/pgbench.c:7768-7822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7768-L7822)

## Overview
A cross-platform function in pgbench that sets up a timer to limit the duration of benchmark execution, with separate implementations for Unix/Linux systems and Windows.

## Definition

```c
static void
setalarm(int seconds)
```
## Detailed Description
This function provides a platform-specific timer mechanism to implement duration-based benchmarking in pgbench. It sets up a timer that will trigger after the specified number of seconds, causing the global  flag to be set, which signals the benchmark execution loop to terminate.

On Unix/Linux systems, it uses POSIX signals (SIGALRM) with the  system call and installs  as the signal handler. On Windows, it uses the Timer Queue API to create a timer that calls the  function when it expires.

The function is typically called once at the beginning of benchmark execution when the  parameter is specified, providing a reliable way to limit test run time regardless of transaction completion.

## Parameters / Member Variables
- : The number of seconds after which the timer should expire and trigger the termination signal

## Dependencies
- Functions called/Symbols referenced:
  - Unix version: , , 
  - Windows version: , , , 
- Called from (representative examples):
  - [main](../m/main.md)() function at src/bin/pgbench/pgbench.c:7347

## Notes and Other Information
- The function has two completely different implementations depending on the target platform
- On Unix systems, it registers a SIGALRM handler and uses the standard  system call
- On Windows, it creates a Timer Queue timer with  flags
- The Windows version includes error handling and calls  if timer creation fails
- The Windows implementation has a safety check to prevent overflow when converting seconds to milliseconds
- The comment "This function will be called at most once" in the Windows version indicates it's designed for single-use in pgbench's execution model
- Both implementations ultimately set the global  flag when the timer expires
- The timer is used in conjunction with pgbench's  (duration) command-line option