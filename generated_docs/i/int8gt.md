# int8gt

## Location
src/backend/utils/adt/int8.c: 140 - 148

## Overview
PostgreSQL function that compares two 64-bit integers and returns true if the first is greater than the second.

## Definition
```c
Datum int8gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8gt` function implements the "greater than" comparison operator (>) for the PostgreSQL bigint (int8) data type. It takes two 64-bit signed integers as arguments through the PostgreSQL function call interface and performs a simple greater-than comparison. The function is part of PostgreSQL's type system infrastructure, providing the underlying implementation for SQL expressions like `bigint_value1 > bigint_value2`.

This function follows PostgreSQL's standard function calling conventions using the `PG_FUNCTION_ARGS` macro to access arguments and `PG_RETURN_BOOL` to return the boolean result.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` calling convention:
  - Argument 0: First 64-bit integer value (`val1`)
  - Argument 1: Second 64-bit integer value (`val2`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Macro to extract 64-bit integer arguments from function call
  - `PG_RETURN_BOOL`: Macro to return boolean result from PostgreSQL function
- Called from (representative examples):
  - SQL engine when evaluating bigint > operations
  - Internal comparison operations in PostgreSQL

## Notes and Other Information
- Located in `src/backend/utils/adt/int8.c:140-148`
- Part of the int8 (bigint) data type implementation
- Simple wrapper around C's `>` operator with PostgreSQL function interface
- Used internally by PostgreSQL's SQL engine for bigint greater-than comparisons
- No overflow or error checking needed as this is a simple comparison operation