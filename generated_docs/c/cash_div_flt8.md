# cash_div_flt8

## Location
[src/backend/utils/adt/cash.c:760-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L760-L772)

## Overview
Divides a PostgreSQL Cash value by a double precision floating-point number (float8), returning the result as a Cash value.

## Definition
```c
Datum cash_div_flt8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator for dividing a money amount by a float8 value. It acts as a wrapper around the internal cash_div_float8 function, handling the PostgreSQL function call interface and argument extraction. The division is performed with rounding to maintain the integer representation of the Cash type, and includes range checking for overflow conditions.

## Parameters / Member Variables
- `c`: The Cash value to be divided (dividend, first argument)
- `f`: The float8 divisor (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH
  - PG_GETARG_FLOAT8
  - [cash_div_float8](cash_div_float8.md)
  - PG_RETURN_CASH
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Delegates actual division logic to cash_div_float8 helper function
- The helper function performs range checking and raises ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE errors for out-of-range results
- [Result](../R/Result.md) is rounded to nearest integer to maintain Cash type precision
- Division by zero is handled by the underlying float8_div function
- Part of PostgreSQL's money data type implementation in src/backend/utils/adt/cash.c