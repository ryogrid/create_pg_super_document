# FloatExceptionHandler

## Location
[src/backend/tcop/postgres.c:3046-3061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3046-L3061)

## Overview
FloatExceptionHandler is a signal handler function that catches floating-point exceptions (SIGFPE) and converts them into PostgreSQL ERROR conditions with detailed error messages.

## Definition
```c
void FloatExceptionHandler(SIGNAL_ARGS)
```

## Detailed Description
FloatExceptionHandler serves as PostgreSQL's signal handler for floating-point exceptions (SIGFPE). When the system detects an invalid floating-point operation such as division by zero, overflow, underflow, or other arithmetic errors, this handler is invoked. Instead of allowing the process to crash or terminate abnormally, the handler converts the floating-point exception into a PostgreSQL ERROR using the ereport mechanism.

The handler generates a comprehensive error message that includes both a general description of the floating-point exception and detailed information about the likely causes. This approach allows PostgreSQL to handle floating-point errors gracefully within its transaction and error recovery framework, rather than having the backend process terminate unexpectedly.

Since this handler calls ereport(ERROR), it will cause the current transaction to abort and control to be transferred to the PostgreSQL error handling system, which can then clean up resources and return an appropriate error to the client.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments (typically signal number and signal info)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (sets error code to ERRCODE_FLOATING_POINT_EXCEPTION)
  - [errmsg](../e/errmsg.md) (sets primary error message)
  - [errdetail](../e/errdetail.md) (provides detailed explanation of the error)
  - SIGNAL_ARGS (macro for signal handler parameters)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main backend process)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (autovacuum worker processes)
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md) (background worker processes)
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (replication slot sync worker)
  - PLPERL_RESTORE_LOCALE (PL/Perl language handler)

## Notes and Other Information
- This is a signal handler function and must be async-signal-safe up to the point where it calls ereport
- The handler does not save errno since it's not returning normally (ereport(ERROR) performs a longjmp)
- Common causes of floating-point exceptions include division by zero, overflow, underflow, and invalid operations
- The handler converts low-level system signals into PostgreSQL's structured error handling mechanism
- Once this handler is triggered, the current transaction will be aborted due to the ERROR level report
- This provides a clean way to handle arithmetic errors that would otherwise crash the backend process

## Simplified Source

```c
// Simplified version of FloatExceptionHandler
void FloatExceptionHandler(SIGNAL_ARGS) {
    // Convert floating-point exception signal to PostgreSQL ERROR
    ereport(ERROR,
            (errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
             errmsg("floating-point exception"),
             errdetail("An invalid floating-point operation was signaled. "
                       "This probably means an out-of-range result or an "
                       "invalid operation, such as division by zero.")));
}
```

Key simplifications made:
- Removed errno handling comment (non-essential detail)
- Preserved the core functionality: signal handling and error reporting
- Maintained the complete error message structure for user clarity
- Focused on the main execution path: signal → error report