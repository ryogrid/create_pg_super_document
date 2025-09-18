# int4inc

## Location
src/backend/utils/adt/int.c: 872 - 885

## Overview
Increments a 32-bit integer by 1 with overflow checking, implementing a PostgreSQL utility function for safe integer increment operations.

## Definition
```c
Datum int4inc(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4inc` function implements an increment operation for PostgreSQL 32-bit integers (int4 type). It takes a single integer argument, adds 1 to it with overflow detection, and returns the result. This function provides a safe way to increment integers by ensuring that incrementing the maximum possible integer value (INT_MAX) results in an error rather than wraparound to a negative value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call context containing:
  - `arg` (int32): The input integer to be incremented by 1

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Macro to extract 32-bit integer argument from function context
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md): Safe addition function that detects overflow when adding 1 to the input
  - `ereport`: PostgreSQL error reporting mechanism
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result
- Called from (representative examples):
  - This function may be used internally within PostgreSQL for sequence operations, counter increments, or other operations requiring safe integer increment

## Notes and Other Information
- Part of PostgreSQLs integer arithmetic operations located in `src/backend/utils/adt/int.c:872-885`
- Uses safe arithmetic to prevent integer overflow, specifically checking if incrementing would exceed INT_MAX (2,147,483,647)
- Follows PostgreSQLs standard function calling conventions using PG_FUNCTION_ARGS
- The overflow check is particularly important for increment operations as they are commonly used in loops and counters where overflow could lead to infinite loops or other unexpected behavior
- This function demonstrates PostgreSQLs commitment to safe arithmetic operations even for simple operations like increment
- The function is essentially equivalent to `int4pl(arg, 1)` but optimized for the common case of incrementing by exactly 1