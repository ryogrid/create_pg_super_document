# errsave_start

## Location
[src/backend/utils/error/elog.c:633-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L633-L684)

## Overview
Begins a "soft" error-reporting cycle that can capture error details without immediately terminating execution, supporting both traditional error handling and error capture scenarios.

## Definition


## Detailed Description
errsave_start provides a flexible error reporting mechanism that supports both traditional error handling and "soft" error capture. When no ErrorSaveContext is provided, it delegates to errstart() for normal ERROR processing. When an ErrorSaveContext is provided, it enables capturing error information without immediate process termination.

The function supports two soft error modes:
1. Notification-only mode: Sets error_occurred flag and returns false to skip detailed error processing
2. Detail capture mode: Creates an error stack entry for collecting full error information

This mechanism is particularly useful for operations that need to attempt potentially-failing actions while capturing error details for later handling, such as data validation or conditional operations.

## Parameters / Member Variables
- : Node pointer that may be an ErrorSaveContext for soft error handling, or NULL for traditional error processing
- : Error domain string for categorizing the error source

## Dependencies
- Functions called/Symbols referenced:
  - [errstart](errstart.md)
  - [get_error_stack_entry](../g/get_error_stack_entry.md)
  - [set_stack_entry_domain](../s/set_stack_entry_domain.md)
- Called from (representative examples):
  - errsave_domain
  - ereturn

## Notes and Other Information
- Returns true to continue error processing, false to skip remaining error steps
- Falls back to errstart(ERROR, domain) when context is NULL or not an ErrorSaveContext
- Uses LOG level internally to signal successful soft error handling to errsave_finish
- Allocates error data in CurrentMemoryContext rather than ErrorContext for soft errors
- Sets error_occurred flag in ErrorSaveContext to notify caller of error detection
- Supports details_wanted flag to control whether full error information is collected
- Uses recursion_depth tracking for consistency with standard error handling