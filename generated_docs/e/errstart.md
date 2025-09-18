# errstart

## Location
src/backend/utils/error/elog.c: 346 - 476

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
  - should_output_to_server
  - should_output_to_client  
  - write_stderr
  - MemoryContextReset
  - in_error_recursion_trouble
  - get_error_stack_entry
  - set_stack_entry_domain
- Called from (representative examples):
  - errstart_cold
  - errsave_start
  - ThrowErrorData
  - ereport_domain

## Notes and Other Information
- Returns true to continue error processing, false to short-circuit reporting
- Implements critical section error promotion (CritSectionCount > 0 forces PANIC)
- Handles three conditions for ERROR → FATAL promotion: no exception handler, ExitOnAnyError mode, proc_exit in progress
- Uses recursion_depth tracking to detect and handle error-during-error scenarios
- Automatically selects appropriate SQL error codes based on error level
- All error state allocations use ErrorContext for proper memory management