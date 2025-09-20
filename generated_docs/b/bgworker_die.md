# bgworker_die

## Location
[src/backend/postmaster/bgworker.c:709-722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L709-L722)

## Overview
A standard SIGTERM signal handler for background worker processes that terminates the worker with a FATAL error message.

## Definition

```c
static void
bgworker_die(SIGNAL_ARGS)
```
## Detailed Description
This function serves as the default signal handler for SIGTERM in background worker processes. When invoked, it performs proper signal handling by masking signals and then terminates the worker process with a FATAL error report that includes the worker type name. This ensures graceful shutdown of background workers when they receive termination signals from the system or administrator commands.

The function follows PostgreSQL's standard pattern for signal handlers by first blocking all signals to prevent race conditions, then reporting the termination reason before exiting.

## Parameters / Member Variables
- Uses  macro which expands to the standard signal handler parameters (typically )

## Dependencies
- Functions called/Symbols referenced:
  - sigprocmask (signal masking)
  - SIG_SETMASK (signal mask operation)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code generation)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Global variables accessed:
  - BlockSig (signal mask)
  - MyBgworkerEntry (current worker's entry)
- Called from:
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md) (registered as SIGTERM handler)

## Notes and Other Information
- This is a static function internal to bgworker.c
- Uses the FATAL error level which causes the process to exit
- The error message includes the worker's bgw_type for identification
- Proper signal handling is ensured by masking all signals before proceeding
- This is the standard way PostgreSQL background workers handle termination requests