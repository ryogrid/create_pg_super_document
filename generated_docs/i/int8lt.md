# int8lt

## Location
[src/backend/utils/adt/int8.c:131-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L131-L139)

## Overview
PostgreSQL function that compares two 64-bit integers and returns true if the first is less than the second.

## Definition
```c
Datum int8lt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8lt` function implements the "less than" comparison operator (<) for the PostgreSQL bigint (int8) data type. It takes two 64-bit signed integers as arguments through the PostgreSQL function call interface and performs a simple less-than comparison. The function is part of PostgreSQL's type system infrastructure, providing the underlying implementation for SQL expressions like `bigint_value1 < bigint_value2`.

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
  - SQL engine when evaluating bigint < operations
  - Internal comparison operations in PostgreSQL

## Notes and Other Information
- Located in `src/backend/utils/adt/int8.c:131-139`
- Part of the int8 (bigint) data type implementation
- Simple wrapper around C's `<` operator with PostgreSQL function interface
- Used internally by PostgreSQL's SQL engine for bigint less-than comparisons
- No overflow or error checking needed as this is a simple comparison operation

## Simplified Source
```c
/*
 * Less than comparison for int8 (bigint) values
 */
Datum int8lt(PG_FUNCTION_ARGS) {
    // Extract two 64-bit integer arguments
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    // Compare if first is less than second and return boolean result
    PG_RETURN_BOOL(val1 < val2);
}
```