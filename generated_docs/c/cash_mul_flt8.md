# cash_mul_flt8

## Location
src/backend/utils/adt/cash.c: 734 - 746

## Overview
Multiplies a PostgreSQL Cash value by a double precision floating-point number (float8), returning the result as a Cash value.

## Definition
```c
Datum cash_mul_flt8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator for multiplying a money amount by a float8 value. It acts as a wrapper around the internal cash_mul_float8 function, handling the PostgreSQL function call interface and argument extraction. The multiplication is performed with rounding to maintain the integer representation of the Cash type.

## Parameters / Member Variables
- `c`: The Cash value to be multiplied (first argument)
- `f`: The float8 multiplier (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH
  - PG_GETARG_FLOAT8
  - [cash_mul_float8](cash_mul_float8.md)
  - PG_RETURN_CASH
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Delegates actual multiplication logic to cash_mul_float8 helper function
- The helper function performs range checking and raises errors for out-of-range results
- [Result](../R/Result.md) is rounded to nearest integer to maintain Cash type precision
- Part of PostgreSQL's money data type implementation in src/backend/utils/adt/cash.c