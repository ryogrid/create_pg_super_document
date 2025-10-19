# float8larger

## Location
[src/backend/utils/adt/float.c:680-693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L680-L693)

## Overview
Returns the larger of two double-precision floating-point numbers in PostgreSQL's SQL function interface.

## Definition
```c
Datum float8larger(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a comparison operation that returns the larger of two PostgreSQL float8 (double-precision floating-point) values. It extracts two float8 values from the function arguments, compares them using the `float8_gt` function, and returns the larger value. This function is typically used to implement SQL's `GREATEST` function or similar maximum-finding operations for double-precision numbers.

## Parameters / Member Variables
- `arg1`: The first input float8 value obtained via `PG_GETARG_FLOAT8(0)`
- `arg2`: The second input float8 value obtained via `PG_GETARG_FLOAT8(1)`
- `result`: Local variable storing the larger of the two input values

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8` (macro for extracting float8 arguments)
  - [float8_gt](float8_gt.md) (function for comparing two float8 values)
  - `PG_RETURN_FLOAT8` (macro for returning float8 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:680-693
- Part of PostgreSQL's float8 base operations suite
- Uses `float8_gt` for comparison to handle NaN and other special floating-point cases properly
- Follows PostgreSQL's standard function call convention with `PG_FUNCTION_ARGS`
- Returns result using PostgreSQL's Datum system
- Implements maximum selection logic for SQL operations

## Simplified Source

```c
Datum
float8larger(PG_FUNCTION_ARGS)
{
    // Extract both float8 arguments
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 arg2 = PG_GETARG_FLOAT8(1);

    // Return the larger value using PostgreSQL's float8 comparison
    if (float8_gt(arg1, arg2))
        PG_RETURN_FLOAT8(arg1);
    else
        PG_RETURN_FLOAT8(arg2);
}
```