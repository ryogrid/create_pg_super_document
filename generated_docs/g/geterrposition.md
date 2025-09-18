# geterrposition

## Location
src/backend/utils/error/elog.c: 1595 - 1611

## Overview
Returns the currently set error cursor position (0 if none) for use in error callback subroutines.

## Definition


## Detailed Description
The  function is designed specifically for use within error callback subroutines to retrieve the current error cursor position. This position typically indicates where in a SQL statement or other text an error occurred. The function accesses the current error data context and returns the cursor position stored there. If no position has been set, it returns 0.

The function is intended only for internal use within error handling contexts, as the concept of error position is not meaningful outside of the error reporting subsystem. It operates without incrementing the recursion depth counter since it's a simple accessor function.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
- Called from (representative examples):
  - [function_parse_error_transpose](../f/function_parse_error_transpose.md)
  - [import_error_callback](../i/import_error_callback.md)
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md)
  - [_SPI_error_callback](../S/_SPI_error_callback.md)
  - [sql_inline_error_callback](../s/sql_inline_error_callback.md)
  - errcontext

## Notes and Other Information
- This function should only be called from error callback subroutines
- Returns 0 if no error position has been set
- Does not increment recursion_depth as it's a simple accessor
- Part of the PostgreSQL error reporting and logging subsystem
- The cursor position typically refers to character positions in SQL statements where errors occurred