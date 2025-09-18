# derfc

## Location
[src/backend/utils/adt/float.c:2765-2831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2765-L2831)

## Overview
PostgreSQL SQL function that computes the complementary error function (erfc) for a floating-point argument, which equals 1 - erf(x).

## Definition


## Detailed Description
The `derfc` function is a PostgreSQL SQL-callable function that wraps the standard C library's `erfc()` function to compute the complementary error function of a floating-point number. The complementary error function is defined as erfc(x) = 1 - erf(x) and is commonly used in probability theory, statistics, and mathematical computations. This function takes a single float8 (double precision) argument and returns the complementary error function value as a float8 result.

Similar to its counterpart `derf`, this function includes overflow checking for robust error handling, though the complementary error function typically doesn't overflow for normal input ranges.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function arguments
- `arg1`: The float8 input value for which to compute the complementary error function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - erfc (standard C library complementary error function)
  - isinf (check for infinite result)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL error handling function)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2765-2831
- Part of PostgreSQL's mathematical function suite alongside `derf`
- The function specifically notes that erfc() never overflows under normal circumstances
- Includes defensive programming with infinity checks despite the low probability of overflow
- Returns standard PostgreSQL Datum type for SQL function compatibility
- Mathematically equivalent to 1 - erf(x) but may provide better numerical accuracy for certain ranges