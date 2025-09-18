# int4_mul_cash

## Location
[src/backend/utils/adt/cash.c:864-877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L864-L877)

## Overview
A PostgreSQL function that multiplies a 32-bit signed integer by a Cash value, providing commutative multiplication support for monetary calculations.

## Definition


## Detailed Description
The `int4_mul_cash` function is a PostgreSQL built-in function that performs multiplication of a 32-bit signed integer (int4) by a Cash data type. This function provides the commutative counterpart to `cash_mul_int4`, allowing multiplication in both directions (Cash * int4 and int4 * Cash). Like its counterpart, it delegates to the internal `cash_mul_int64` helper function after promoting the int4 parameter to int64 for consistent arithmetic handling.

The function follows PostgreSQL's standard function calling convention and ensures safe multiplication operations without integer overflow issues by using the 64-bit internal helper.

## Parameters / Member Variables
- `i` (int32): The 32-bit signed integer multiplier (first argument)
- `c` (Cash): The monetary value to be multiplied (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Extracts int32 value from function arguments
  - `PG_GETARG_CASH`: Extracts Cash value from function arguments
  - [cash_mul_int64](../c/cash_mul_int64.md): Internal helper function for safe Cash multiplication
  - `PG_RETURN_CASH`: Returns Cash value as function result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function provides commutative multiplication support, allowing both `Cash * int4` and `int4 * Cash` operations
- Delegates to the same `cash_mul_int64` helper as `cash_mul_int4`, ensuring consistent behavior regardless of operand order
- Part of PostgreSQL's comprehensive monetary arithmetic type system
- The parameter order differs from `cash_mul_int4` but the internal implementation uses the same multiplication logic