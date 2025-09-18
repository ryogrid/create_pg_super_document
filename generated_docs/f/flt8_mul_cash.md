# flt8_mul_cash

## Location
src/backend/utils/adt/cash.c: 747 - 759

## Overview
Multiplies a double precision floating-point number (float8) by a PostgreSQL Cash value, returning the result as a Cash value.

## Definition
```c
Datum flt8_mul_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator for multiplying a float8 value by a money amount. It provides the commutative operation to cash_mul_flt8, allowing float8 * cash syntax in addition to cash * float8. Like its counterpart, it delegates to the internal cash_mul_float8 function to perform the actual multiplication with proper rounding and range checking.

## Parameters / Member Variables
- `f`: The float8 multiplier (first argument)
- `c`: The Cash value to be multiplied (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - PG_GETARG_CASH
  - cash_mul_float8
  - PG_RETURN_CASH
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Provides commutative multiplication operation (float8 * cash vs cash * float8)
- Uses the same underlying cash_mul_float8 implementation as cash_mul_flt8
- Arguments are in reverse order compared to cash_mul_flt8 but produce identical results due to multiplication commutativity
- Part of PostgreSQL's money data type implementation in src/backend/utils/adt/cash.c