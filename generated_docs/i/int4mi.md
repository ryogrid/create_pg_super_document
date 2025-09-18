# int4mi

## Location
src/backend/utils/adt/int.c: 805 - 818

## Overview
Performs subtraction of two 32-bit integers with overflow checking, implementing the PostgreSQL SQL function for integer subtraction.

## Definition
```c
Datum int4mi(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4mi` function implements the subtraction operation for PostgreSQL 32-bit integers (int4 type). It extracts two integer arguments from the function call context, performs subtraction with overflow detection, and returns the result. If the subtraction would cause an integer overflow, the function raises an error with an appropriate error code and message rather than allowing undefined behavior.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call context containing:
  - `arg1` (int32): The first operand (minuend) - the number from which another is subtracted
  - `arg2` (int32): The second operand (subtrahend) - the number to be subtracted

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Macro to extract 32-bit integer arguments from function context
  - `pg_sub_s32_overflow`: Safe subtraction function that detects overflow conditions
  - `ereport`: PostgreSQL error reporting mechanism
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result
- Called from (representative examples):
  - This function is typically invoked through PostgreSQLs SQL function dispatch system when the subtraction operator (-) is used with integer types

## Notes and Other Information
- Part of PostgreSQLs integer arithmetic operations located in `src/backend/utils/adt/int.c:805-818`
- Uses safe arithmetic to prevent integer overflow, which is crucial for data integrity
- Follows PostgreSQLs standard function calling conventions using PG_FUNCTION_ARGS
- The overflow check ensures that operations like `INT_MIN - 1` or `INT_MAX - (-1)` are properly handled with error reporting rather than wraparound behavior