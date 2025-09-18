# errsave_finish

## Location
[src/backend/utils/error/elog.c:685-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L685-L754)

## Overview
Finalizes a "soft" error-reporting cycle by either delegating to standard error processing or packaging error details for caller access without process termination.

## Definition


## Detailed Description
errsave_finish completes the soft error reporting cycle initiated by errsave_start(). It handles two distinct scenarios based on how the error was initially processed. If errsave_start() determined this was a regular error (ERROR level or higher), it delegates to errfinish() for standard error processing including potential process termination. If this was truly a soft error (LOG level), it packages the collected error information and stores it in the ErrorSaveContext for the caller to examine.

For soft errors, the function performs minimal processing compared to errfinish() - it deliberately skips backtrace collection and context callback execution to avoid side effects that assume transaction abort. Instead, it creates a copy of the error data for the caller while ensuring all subsidiary strings remain accessible in the caller's memory context.

## Parameters / Member Variables
- : ErrorSaveContext node where error details will be stored for soft errors
- : Source file where the error occurred
- : Line number in the source file where the error occurred
- : Function name where the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - [errfinish](errfinish.md)
  - pg_unreachable
  - [set_stack_entry_location](../s/set_stack_entry_location.md)
  - palloc_object
- Called from (representative examples):
  - errsave_domain
  - ereturn

## Notes and Other Information
- Does not return for ERROR level or higher errors - delegates to errfinish() which may terminate
- Updates error level from LOG (set by errsave_start) to ERROR for proper reporting
- Skips backtrace and context callbacks to avoid side effects during soft error handling
- Creates a flat copy of ErrorData since subsidiary strings are already in caller's context
- Uses recursion_depth tracking for consistency with standard error handling
- Assumes ErrorSaveContext is properly initialized and error_occurred flag was set
- Memory allocation for error_data copy uses caller's current memory context