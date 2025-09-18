# numeric_div_trunc

## Location
src/backend/utils/adt/numeric.c: 3275 - 3363

## Overview
PostgreSQL function that performs division of two numeric values and truncates the result to an integer, effectively implementing floor division.

## Definition
```c
Datum numeric_div_trunc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements truncated division (floor division) for PostgreSQL's Numeric data type. It divides the first argument by the second and truncates the result to an integer by calling div_var with scale 0 and round=false. The function handles special numeric values (NaN, infinity) according to IEEE standards and PostgreSQL conventions.

Key behaviors:
- Performs division and truncates result to integer (no fractional part)
- Handles special cases: NaN propagation, infinity division rules
- Throws division by zero errors when appropriate
- Returns integer result as Numeric type
- Uses PostgreSQL's function calling convention (PG_FUNCTION_ARGS)

## Parameters / Member Variables
- Function arguments accessed via PG_GETARG_NUMERIC():
  - Argument 0: The dividend (numerator) - the Numeric value to be divided
  - Argument 1: The divisor (denominator) - the Numeric value to divide by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC, PG_RETURN_NUMERIC
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_PINF, NUMERIC_IS_NINF
  - make_result, numeric_sign_internal
  - init_var_from_num, init_var, free_var
  - div_var (called with scale=0, round=false for truncation)
- Called from (representative examples):
  - numeric_half_rounded
  - numeric_truncated_divide

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL
- The truncation behavior differs from regular division which preserves decimal precision
- Used in database size calculation functions for converting between units
- Special value handling follows the same rules as regular numeric division
- The result is always an integer value represented as Numeric type