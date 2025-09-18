# int2um

## Location
src/backend/utils/adt/int.c: 886 - 897

## Overview
Performs unary minus (negation) operation on a 16-bit integer with overflow checking, implementing the PostgreSQL SQL function for smallint negation.

## Definition
```c
Datum int2um(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2um` function implements the unary minus operation for PostgreSQL 16-bit integers (smallint/int2 type). It takes a single smallint argument and returns its negation. The function includes special handling for the edge case where negating the minimum possible smallint value would cause an overflow, since in twos complement arithmetic, the absolute value of the minimum negative number cannot be represented as a positive number in the same bit width.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call context containing:
  - `arg` (int16): The input 16-bit integer to be negated

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Macro to extract 16-bit integer argument from function context
  - `PG_INT16_MIN`: Constant representing the minimum value for 16-bit signed integers (-32,768)
  - `ereport`: PostgreSQL error reporting mechanism
  - `PG_RETURN_INT16`: Macro to return 16-bit integer result
- Called from (representative examples):
  - This function is typically invoked through PostgreSQLs SQL function dispatch system when the unary minus operator (-) is used with smallint types

## Notes and Other Information
- Part of PostgreSQLs integer arithmetic operations located in `src/backend/utils/adt/int.c:886-897`
- Handles the special case where `-(-32768)` cannot be represented in 16-bit signed arithmetic, raising a "smallint out of range" error
- The overflow condition occurs because in twos complement representation, the range is [-32768, 32767], so negating -32768 would require +32768 which exceeds the positive range
- Follows PostgreSQLs standard function calling conventions using PG_FUNCTION_ARGS
- This function demonstrates the careful attention PostgreSQL pays to mathematical edge cases even in simple operations
- The "um" suffix likely stands for "unary minus" to distinguish it from binary subtraction operations
- Uses the `unlikely()` macro to optimize for the common case where the input is not the minimum value