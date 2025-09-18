# int8div

## Location
src/backend/utils/adt/int8.c: 504 - 545

## Overview
Performs division of two 64-bit signed integers (bigint) with proper error handling for division by zero and overflow conditions.

## Definition


## Detailed Description
The int8div function implements the division operation for PostgreSQL's bigint data type. It extracts two int64 arguments from the function arguments, performs division with comprehensive error checking, and returns the result. The function handles two critical edge cases: division by zero (which raises an error) and division by -1 when the dividend is INT64_MIN (which would cause overflow in two's complement arithmetic). The function uses PostgreSQL's standard function calling convention with PG_FUNCTION_ARGS and returns a Datum.

## Parameters / Member Variables
- Uses  macro to access function arguments
- : First operand (dividend) extracted as int64
- : Second operand (divisor) extracted as int64
- : Stores the division result as int64

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts int64 arguments)
  - PG_RETURN_INT64 (returns int64 result)
  - PG_RETURN_NULL (returns NULL on error path)
  - PG_INT64_MIN (minimum int64 constant)
  - ereport (error reporting)
  - [errcode](../e/errcode.md)/errmsg (error code and message macros)
- Called from:
  - No direct references found (likely called via PostgreSQL function dispatch system)

## Notes and Other Information
- Implements division by -1 as negation to avoid two's complement overflow issues
- Division by zero raises ERRCODE_DIVISION_BY_ZERO error
- INT64_MIN / -1 raises ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error
- Uses unlikely() hint for the overflow case to optimize common path
- Compiler workaround comment indicates potential GCC optimization issues with unreachable code after error reporting