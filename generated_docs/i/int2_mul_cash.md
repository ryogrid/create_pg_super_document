# int2_mul_cash

## Location
src/backend/utils/adt/cash.c: 903 - 915

## Overview
A PostgreSQL function that multiplies a 16-bit signed integer by a Cash value, providing commutative multiplication support for monetary calculations with small integer values.

## Definition


## Detailed Description
The `int2_mul_cash` function is a PostgreSQL built-in function that performs multiplication of a 16-bit signed integer (int2) by a Cash data type. This function provides the commutative counterpart to `cash_mul_int2`, allowing multiplication in both directions (Cash * int2 and int2 * Cash). Like its counterpart, it delegates to the internal `cash_mul_int64` helper function after promoting the int2 parameter to int64 for consistent arithmetic handling.

The function ensures that multiplication operations are commutative and maintains the same safe arithmetic guarantees across different operand orders, supporting PostgreSQL's comprehensive monetary calculation system.

## Parameters / Member Variables
- `s` (int16): The 16-bit signed integer multiplier (first argument)
- `c` (Cash): The monetary value to be multiplied (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Extracts int16 value from function arguments
  - `PG_GETARG_CASH`: Extracts Cash value from function arguments
  - [cash_mul_int64](../c/cash_mul_int64.md): Internal helper function for safe Cash multiplication
  - `PG_RETURN_CASH`: Returns Cash value as function result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function provides commutative multiplication support, allowing both `Cash * int2` and `int2 * Cash` operations
- Delegates to the same `cash_mul_int64` helper as `cash_mul_int2`, ensuring consistent behavior regardless of operand order
- Part of PostgreSQL's comprehensive monetary arithmetic type system supporting various integer sizes
- The parameter order differs from `cash_mul_int2` but the internal implementation uses the same multiplication logic
- Completes the set of commutative multiplication functions for different integer sizes (int2, int4) with Cash values