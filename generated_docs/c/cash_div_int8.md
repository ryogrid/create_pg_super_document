# cash_div_int8

## Location
src/backend/utils/adt/cash.c: 838 - 850

## Overview
A PostgreSQL function that performs division of a Cash value by a 64-bit integer (int8), returning the result as a Cash type.

## Definition
```c
Datum cash_div_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the division operation between a PostgreSQL Cash value and a 64-bit integer (int8/bigint). It serves as a wrapper function that extracts the arguments and delegates the actual computation to the `cash_div_int64` helper function. This design provides a clean interface for SQL-level division operations while leveraging the robust integer arithmetic implementation with proper division-by-zero checking and safe computation in the helper function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Cash value (dividend) to be divided
  - Argument 1: int64 value (divisor) to divide by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call context
  - PG_GETARG_INT64: Extracts int64 argument from function call context
  - cash_div_int64: Performs the actual safe division with division-by-zero checking
  - PG_RETURN_CASH: Returns the computed Cash result
- Called from:
  - SQL operator implementations for money / bigint operations

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:838-850
- Part of PostgreSQL's monetary data type arithmetic operations
- Leverages the cash_div_int64 helper function which provides division-by-zero protection and safe arithmetic
- Handles large integer divisors efficiently through direct 64-bit integer arithmetic
- Related to the already processed cash_div_int64 helper function that provides safe division with proper error handling
- Complements the multiplication operations (cash_mul_int8, int8_mul_cash) for complete integer arithmetic support