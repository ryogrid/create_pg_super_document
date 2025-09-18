# flt4_mul_cash

## Location
src/backend/utils/adt/cash.c: 786 - 799

## Overview
A PostgreSQL function that performs multiplication of a float4 value by a Cash value, returning the result as a Cash type.

## Definition
```c
Datum flt4_mul_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the multiplication operation between a single-precision floating-point number (float4) and a PostgreSQL Cash value. It serves as a wrapper function that extracts the arguments, converts the float4 to float8 for precision, and delegates the actual computation to the `cash_mul_float8` helper function. This design ensures consistent handling of floating-point arithmetic with Cash values across different precision levels.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: float4 value (single-precision floating-point multiplier)
  - Argument 1: Cash value to be multiplied

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4: Extracts float4 argument from function call context
  - PG_GETARG_CASH: Extracts Cash argument from function call context
  - cash_mul_float8: Performs the actual multiplication with float8 precision
  - PG_RETURN_CASH: Returns the computed Cash result
- Called from:
  - SQL operator implementations for float4 * money operations

## Notes and Other Information
- The function promotes the float4 input to float8 before computation to maintain precision consistency
- Located in src/backend/utils/adt/cash.c:786-799
- Part of PostgreSQL's monetary data type arithmetic operations
- Handles overflow and underflow through the underlying cash_mul_float8 implementation