# cash_mul_flt4

## Location
src/backend/utils/adt/cash.c: 773 - 785

## Overview
Multiplies a PostgreSQL Cash value by a single precision floating-point number (float4), returning the result as a Cash value.

## Definition
```c
Datum cash_mul_flt4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator for multiplying a money amount by a float4 value. It converts the float4 argument to float8 and then delegates to the cash_mul_float8 function for the actual computation. This approach ensures consistent behavior and precision across different floating-point multiplication operations with Cash values.

## Parameters / Member Variables
- `c`: The Cash value to be multiplied (first argument)
- `f`: The float4 multiplier (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH
  - PG_GETARG_FLOAT4
  - [cash_mul_float8](cash_mul_float8.md)
  - PG_RETURN_CASH
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Converts float4 to float8 before performing multiplication to use the common cash_mul_float8 implementation
- Provides support for single precision floating-point multiplication with money values
- Uses the same underlying logic and error handling as cash_mul_flt8
- Part of PostgreSQL's money data type implementation in src/backend/utils/adt/cash.c