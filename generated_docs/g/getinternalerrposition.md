# getinternalerrposition

## Location
src/backend/utils/error/elog.c: 1612 - 1644

## Overview
Returns the currently set internal error position (0 if none) for use in error callback subroutines.

## Definition
```c
int getinternalerrposition(void)
```

## Detailed Description
The `getinternalerrposition` function is the companion to `geterrposition`, specifically designed to retrieve the internal error cursor position. This position typically indicates where within internally generated SQL statements or queries an error occurred, as opposed to user-provided SQL text. Like its counterpart, this function is intended only for use within error callback subroutines where the concept of internal error position is meaningful.

The function accesses the current error data context and returns the internal position stored there. If no internal position has been set, it returns 0. The function operates without incrementing the recursion depth counter since it's a simple accessor function.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - function_parse_error_transpose
  - errcontext

## Notes and Other Information
- This function should only be called from error callback subroutines
- Returns 0 if no internal error position has been set
- Does not increment recursion_depth as it's a simple accessor
- Specifically handles internal error positions (vs. user SQL positions)
- Part of the PostgreSQL error reporting and logging subsystem
- Used for errors in internally generated SQL or system queries