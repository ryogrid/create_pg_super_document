# pgwin32_deadchild_callback

## Location
src/backend/postmaster/postmaster.c: 4648 - 4676

## Overview
A Windows thread pool callback function that handles child process termination events by posting completion status and queuing SIGCHLD signals.

## Definition
```c
static void WINAPI pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
```

## Detailed Description
This function serves as a Windows API callback executed on a thread pool when a child process terminates. It receives a win32_deadchild_waitinfo object as a parameter and posts it to a completion queue for the waitpid() function to process. After posting the completion status, it queues a SIGCHLD signal to notify the main thread about the child process termination. The function includes safeguards against timer-fired events (which shouldn't occur) and error handling for completion status posting failures.

## Parameters / Member Variables
- `lpParameter`: A PVOID pointer to win32_deadchild_waitinfo structure containing child process information
- `TimerOrWaitFired`: Boolean indicating whether the callback was triggered by a timer (TRUE) or wait object (FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - PostQueuedCompletionStatus (Windows API for posting completion status)
  - [write_stderr](../w/write_stderr.md) (PostgreSQL error reporting function)
  - [pg_queue_signal](pg_queue_signal.md) (PostgreSQL signal queuing function)
  - SIGCHLD (signal constant for child process termination)
- Called from:
  - [pgwin32_register_deadchild_callback](pgwin32_register_deadchild_callback.md) (src/backend/postmaster/postmaster.c:4687)

## Notes and Other Information
- This function executes on a Windows thread pool, requiring all operations to be thread-safe
- Standard PostgreSQL logging functions like elog() cannot be used from this thread context
- Uses INFINITE as timeout value, so TimerOrWaitFired should never be TRUE
- If PostQueuedCompletionStatus fails, the function leaks the win32_deadchild_waitinfo object
- The function is declared as WINAPI, indicating it follows Windows calling conventions
- This is a static function, only accessible within postmaster.c
- Part of the Windows-specific child process management system in PostgreSQL
- Located in src/backend/postmaster/postmaster.c:4648-4676