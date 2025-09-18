# cash_div_int4

## Location
[src/backend/utils/adt/cash.c:878-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L878-L890)

## Overview
A PostgreSQL function that divides a Cash value by a 32-bit signed integer, providing safe division operations for monetary calculations.

## Definition


## Detailed Description
The `cash_div_int4` function is a PostgreSQL built-in function that performs division of a Cash data type by a 32-bit signed integer (int4). This function serves as a wrapper around the internal `cash_div_int64` helper function, promoting the int4 divisor to int64 for consistent internal arithmetic handling. It ensures safe division operations with proper error handling for division by zero and maintains precision in monetary calculations.

The function follows PostgreSQL's standard function calling convention and is part of the comprehensive monetary arithmetic operations provided by the Cash data type.

## Parameters / Member Variables
- `c` (Cash): The monetary value to be divided (dividend, first argument)
- `i` (int32): The 32-bit signed integer divisor (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CASH`: Extracts Cash value from function arguments
  - `PG_GETARG_INT32`: Extracts int32 value from function arguments
  - [cash_div_int64](cash_div_int64.md): Internal helper function for safe Cash division
  - `PG_RETURN_CASH`: Returns Cash value as function result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function delegates the actual division logic to `cash_div_int64` by promoting the int4 parameter to int64
- Part of PostgreSQL's type system for safe monetary arithmetic operations
- The underlying `cash_div_int64` helper handles division by zero errors and maintains proper precision
- Follows the standard PostgreSQL function interface pattern for built-in mathematical functions
- Unlike multiplication, division is not commutative, so there is no corresponding `int4_div_cash` function