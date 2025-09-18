# int4larger

## Location
src/backend/utils/adt/int.c: 1364 - 1372

## Overview
Returns the larger of two 32-bit signed integers (int32).

## Definition
```c
Datum int4larger(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4larger` function is a PostgreSQL built-in function that compares two 32-bit signed integers and returns the larger value. It implements the MAX operation for the `int4` (integer) data type. The function uses PostgreSQL's function calling convention with `PG_FUNCTION_ARGS` and returns a `Datum` value containing the result.

The implementation is straightforward: it extracts two int32 arguments from the function call arguments, compares them using a simple conditional expression, and returns the larger value wrapped in a PostgreSQL Datum.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` convention:
  - `arg1`: First 32-bit signed integer (extracted from argument 0)
  - `arg2`: Second 32-bit signed integer (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32` - Macro to extract int32 arguments from function call
  - `PG_RETURN_INT32` - Macro to return int32 value as Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1364-1372`
- This function corresponds to the SQL `GREATEST()` function when used with two integer values
- Part of PostgreSQL's arithmetic and comparison operators for the int4/integer data type
- Uses standard PostgreSQL V1 function calling convention
- The comparison is performed using simple C conditional operator for efficiency