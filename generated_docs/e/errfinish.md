# errfinish

## Location
[src/backend/utils/error/elog.c:477-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L477-L632)

## Overview
Finalizes an error-reporting cycle by processing the error report, handling error level-specific actions, and managing control flow including process termination for severe errors.

## Definition

```c
void
errfinish(const char *filename, int lineno, const char *funcname)
```
## Detailed Description
errfinish completes the error reporting cycle initiated by errstart(). It handles the final processing of error messages including backtrace collection, context callback execution, error emission, and error level-specific recovery actions. For ERROR level, it performs cleanup and uses longjmp to transfer control to the appropriate exception handler. For FATAL errors, it triggers process termination via proc_exit(). For PANIC errors, it immediately aborts the process.

Key responsibilities include:
- Setting source location information (filename, line number, function name)
- Collecting backtraces when enabled and appropriate
- Executing error context callbacks for additional error information
- Handling ERROR level by cleaning up state and throwing to exception handlers
- Emitting error reports to appropriate destinations (server log, client)
- Managing memory context switching and cleanup
- Implementing different termination strategies based on error severity

## Parameters / Member Variables
- `*filename`: Source file where the error occurred
- `lineno`: Line number in the source file where the error occurred
- `*funcname`: Function name where the error occurred
## Dependencies
- Functions called/Symbols referenced:
  - [set_stack_entry_location](../s/set_stack_entry_location.md)
  - [matches_backtrace_functions](../m/matches_backtrace_functions.md)
  - [set_backtrace](../s/set_backtrace.md)
  - PG_RE_THROW
  - [EmitErrorReport](../E/EmitErrorReport.md)
  - [FreeErrorDataContents](../F/FreeErrorDataContents.md)
  - [proc_exit](../p/proc_exit.md)
- Called from (representative examples):
  - [errsave_finish](errsave_finish.md)
  - [ThrowErrorData](../T/ThrowErrorData.md)
  - [pg_re_throw](../p/pg_re_throw.md)
  - ereport_domain

## Notes and Other Information
- Does not return to caller for ERROR level or higher - uses longjmp or process termination
- Performs minimal cleanup before ERROR longjmp including resetting interrupt holdoff counts and critical section count
- For FATAL errors, updates session termination statistics and calls proc_exit(1)
- For PANIC errors, flushes output and calls abort() for immediate termination
- Executes in ErrorContext to ensure sufficient memory for error processing
- Handles error recursion through recursion_depth tracking
- Always checks for interrupts after non-fatal error processing to allow query cancellation

## Simplified Source

```c
void
errfinish(const char *filename, int lineno, const char *funcname)
{
    ErrorData *edata = &errordata[errordata_stack_depth];
    int elevel = edata->elevel;

    // Set location information
    set_stack_entry_location(edata, filename, lineno, funcname);

    // Switch to ErrorContext for processing
    MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);

    // Execute error context callbacks
    for (ErrorContextCallback *econtext = error_context_stack;
         econtext != NULL; econtext = econtext->previous)
        econtext->callback(econtext->arg);

    // Handle different error levels
    if (elevel == ERROR) {
        // Clean up state and longjmp to handler
        InterruptHoldoffCount = 0;
        QueryCancelHoldoffCount = 0;
        CritSectionCount = 0;
        recursion_depth--;
        PG_RE_THROW();
    }

    // Emit error report
    EmitErrorReport();

    // Clean up and restore context
    FreeErrorDataContents(edata);
    errordata_stack_depth--;
    MemoryContextSwitchTo(oldcontext);
    recursion_depth--;

    // Handle FATAL/PANIC levels
    if (elevel == FATAL) {
        proc_exit(1);
    }
    if (elevel >= PANIC) {
        abort();
    }

    CHECK_FOR_INTERRUPTS();
}
```