# errstart

## Location
[src/backend/utils/error/elog.c:346-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L346-L476)

## Overview
Begins an error-reporting cycle by creating and initializing an error stack entry, handling error level promotion logic, and determining whether the error should be processed.

## Definition


## Detailed Description
errstart is the core function that initiates PostgreSQL's error reporting mechanism. It creates and initializes an error stack entry that will subsequently be populated by functions like errmsg() before being finalized by errfinish(). The function implements sophisticated error level promotion logic, handles error recursion scenarios, and determines whether an error should be output to the server log, client, or both.

Key responsibilities include:
- Promoting errors to more severe levels in critical sections (ERROR → PANIC)
- Converting ERROR to FATAL in specific conditions (no exception handler, ExitOnAnyError mode, or during proc_exit)
- Preventing error level downgrades when higher-severity errors are already stacked
- Managing error recursion and recovery from error-during-error scenarios
- Optimizing performance by short-circuiting low-level messages that won't be reported

## Parameters / Member Variables
- : Error severity level (DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC)
- : Error domain string for categorizing the error source

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