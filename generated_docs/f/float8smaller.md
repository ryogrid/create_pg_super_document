# float8smaller

## Location
[src/backend/utils/adt/float.c:694-720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L694-L720)

## Overview
Returns the smaller of two double-precision floating-point numbers in PostgreSQL's SQL function interface.

## Definition
```c
Datum float8smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a comparison operation that returns the smaller of two PostgreSQL float8 (double-precision floating-point) values. It extracts two float8 values from the function arguments, compares them using the `float8_lt` function, and returns the smaller value. This function is typically used to implement SQL's `LEAST` function or similar minimum-finding operations for double-precision numbers.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access arguments
- `arg1`: The first input float8 value obtained via `PG_GETARG_FLOAT8(0)`
- `arg2`: The second input float8 value obtained via `PG_GETARG_FLOAT8(1)`
- `result`: Local variable storing the smaller of the two input values

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8` (macro for extracting float8 arguments)
  - [float8_lt](float8_lt.md) (function for comparing two float8 values)
  - `PG_RETURN_FLOAT8` (macro for returning float8 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:694-720
- Part of PostgreSQL's float8 base operations suite
- Uses `float8_lt` for comparison to handle NaN and other special floating-point cases properly
- Follows PostgreSQL's standard function call convention with `PG_FUNCTION_ARGS`
- Returns result using PostgreSQL's Datum system
- Implements minimum selection logic for SQL operations
- Complementary function to `float8larger`