# errstart

## Location
[src/backend/utils/error/elog.c:346-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L346-L476)

## Overview
Begins an error-reporting cycle by creating and initializing an error stack entry, handling error level promotion logic, and determining whether the error should be processed.

## Definition

```c
bool
errstart(int elevel, const char *domain)
```
## Detailed Description
errstart is the core function that initiates PostgreSQL's error reporting mechanism. It creates and initializes an error stack entry that will subsequently be populated by functions like errmsg() before being finalized by errfinish(). The function implements sophisticated error level promotion logic, handles error recursion scenarios, and determines whether an error should be output to the server log, client, or both.

Key responsibilities include:
- Promoting errors to more severe levels in critical sections (ERROR → PANIC)
- Converting ERROR to FATAL in specific conditions (no exception handler, ExitOnAnyError mode, or during proc_exit)
- Preventing error level downgrades when higher-severity errors are already stacked
- Managing error recursion and recovery from error-during-error scenarios
- Optimizing performance by short-circuiting low-level messages that won't be reported

## Parameters / Member Variables
- `elevel`: Error severity level (DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC)
- `*domain`: Error domain string for categorizing the error source
## Dependencies
- Functions called/Symbols referenced:
  - [should_output_to_server](../s/should_output_to_server.md)
  - [should_output_to_client](../s/should_output_to_client.md)  
  - [write_stderr](../w/write_stderr.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [in_error_recursion_trouble](../i/in_error_recursion_trouble.md)
  - [get_error_stack_entry](../g/get_error_stack_entry.md)
  - [set_stack_entry_domain](../s/set_stack_entry_domain.md)
- Called from (representative examples):
  - [errstart_cold](errstart_cold.md)
  - [errsave_start](errsave_start.md)
  - [ThrowErrorData](../T/ThrowErrorData.md)
  - ereport_domain

## Notes and Other Information
- Returns true to continue error processing, false to short-circuit reporting
- Implements critical section error promotion (CritSectionCount > 0 forces PANIC)
- Handles three conditions for ERROR → FATAL promotion: no exception handler, ExitOnAnyError mode, proc_exit in progress
- Uses recursion_depth tracking to detect and handle error-during-error scenarios
- Automatically selects appropriate SQL error codes based on error level
- All error state allocations use ErrorContext for proper memory management

## Simplified Source

```c
bool
errstart(int elevel, const char *domain)
{
    // Check for error level promotion
    if (elevel >= ERROR) {
        if (CritSectionCount > 0)
            elevel = PANIC;  // Critical section errors become PANIC

        if (elevel == ERROR) {
            // Convert ERROR to FATAL in specific conditions
            if (PG_exception_stack == NULL || ExitOnAnyError || proc_exit_inprogress)
                elevel = FATAL;
        }

        // Prevent downgrading stacked errors
        for (int i = 0; i <= errordata_stack_depth; i++)
            elevel = Max(elevel, errordata[i].elevel);
    }

    // Check if we should process this error
    bool output_to_server = should_output_to_server(elevel);
    bool output_to_client = should_output_to_client(elevel);
    if (elevel < ERROR && !output_to_server && !output_to_client)
        return false;

    // Handle error recursion
    if (recursion_depth++ > 0 && elevel >= ERROR) {
        MemoryContextReset(ErrorContext);
        if (in_error_recursion_trouble()) {
            error_context_stack = NULL;
            debug_query_string = NULL;
        }
    }

    // Initialize error data
    ErrorData *edata = get_error_stack_entry();
    edata->elevel = elevel;
    edata->output_to_server = output_to_server;
    edata->output_to_client = output_to_client;
    set_stack_entry_domain(edata, domain);

    recursion_depth--;
    return true;
}
```