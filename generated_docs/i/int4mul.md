# int4mul

## Location
src/backend/utils/adt/int.c: 819 - 832

## Overview
Performs multiplication of two 32-bit integers with overflow checking, implementing the PostgreSQL SQL function for integer multiplication.

## Definition
```c
Datum int4mul(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4mul` function implements the multiplication operation for PostgreSQL 32-bit integers (int4 type). It extracts two integer arguments from the function call context, performs multiplication with overflow detection, and returns the result. If the multiplication would cause an integer overflow, the function raises an error with an appropriate error code and message to prevent data corruption and maintain mathematical correctness.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call context containing:
  - `arg1` (int32): The first operand (multiplicand) - the number to be multiplied
  - `arg2` (int32): The second operand (multiplier) - the number by which to multiply

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Macro to extract 32-bit integer arguments from function context
  - `pg_mul_s32_overflow`: Safe multiplication function that detects overflow conditions
  - `ereport`: PostgreSQL error reporting mechanism
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result
- Called from (representative examples):
  - This function is typically invoked through PostgreSQLs SQL function dispatch system when the multiplication operator (*) is used with integer types

## Notes and Other Information
- Part of PostgreSQLs integer arithmetic operations located in `src/backend/utils/adt/int.c:819-832`
- Uses safe arithmetic to prevent integer overflow, which is especially important for multiplication as it can easily exceed 32-bit limits
- Follows PostgreSQLs standard function calling conventions using PG_FUNCTION_ARGS
- The overflow check ensures that operations that would exceed the range of 32-bit signed integers (approximately ±2.1 billion) are properly handled with error reporting
- Multiplication overflow is particularly common and dangerous, making this check essential for data integrity