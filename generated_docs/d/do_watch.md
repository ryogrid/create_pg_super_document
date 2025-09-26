# do_watch

## Location
[src/bin/psql/command.c:5333-5573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5333-L5573)

## Overview
Implements the PostgreSQL psql \watch command functionality, repeatedly executing a query at specified intervals with optional iteration limits and minimum row constraints.

## Definition

```c
struct itimerval interval;
```
## Detailed Description
The  function provides the core implementation for psql's \watch command, which repeatedly executes a SQL query at regular intervals. It handles cross-platform timing mechanisms, signal management for graceful interruption, and optional pager integration for output display.

The function sets up interval timers (Unix) or uses pg_usleep loops (Windows) to control execution timing. On Unix systems, it uses signal handling (SIGALRM, SIGINT, SIGCHLD) for precise timing and clean interruption. It supports optional pager integration via PSQL_WATCH_PAGER environment variable and includes sophisticated title generation with timestamps for each execution.

The implementation includes robust error handling, iteration counting, minimum row filtering, and proper cleanup of resources including timers, signal masks, and pager processes.

## Parameters / Member Variables
- : PQExpBuffer containing the SQL query to execute repeatedly
- : Time interval between executions in seconds (converted to milliseconds internally)
- : Maximum number of iterations to perform (0 = infinite)
- : Minimum number of rows required to continue execution

## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](../p/printQueryOpt.md) (query output formatting options)
  - sigset_t, sigemptyset, sigaddset, sigprocmask (Unix signal management)
  - [setitimer](../s/setitimer.md), ITIMER_REAL (Unix interval timer)
  - [disable_sigpipe_trap](disable_sigpipe_trap.md), restore_sigpipe_trap (signal handling utilities)
  - popen, pclose (pager process management)
  - [PSQLexecWatch](../P/PSQLexecWatch.md) (executes the query with watch-specific handling)
  - [pg_usleep](../p/pg_usleep.md) (Windows sleep implementation)
  - [pg_malloc](../p/pg_malloc.md), pg_free (PostgreSQL memory management)
- Called from (representative examples):
  - [exec_command_watch](../e/exec_command_watch.md) (handles \watch command parsing and delegation)

## Notes and Other Information
- Uses different timing strategies: Unix systems use SIGALRM with setitimer, Windows uses pg_usleep loops
- Supports PSQL_WATCH_PAGER environment variable for custom pager integration
- Includes comprehensive signal handling for SIGINT (Ctrl+C), SIGCHLD (pager exit), and SIGALRM (timer)
- Generates timestamped titles for each execution showing current time and interval
- Handles pager errors gracefully and restores terminal state appropriately
- Implements iteration counting and minimum row filtering for conditional execution
- Cross-platform compatible with platform-specific optimizations for timing and signal handling