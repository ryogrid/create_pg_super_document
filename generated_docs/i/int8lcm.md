# int8lcm

## Location
[src/backend/utils/adt/int8.c:682-718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L682-L718)

## Overview
Computes the Least Common Multiple (LCM) of two 64-bit signed integers, with overflow detection and error handling.

## Definition


## Detailed Description
This function calculates the least common multiple of two int64 values using the mathematical formula: lcm(x, y) = abs(x / gcd(x, y) * y). It implements several safety measures including special case handling for zero arguments, overflow detection during multiplication, and range validation to ensure the result can be represented as a PostgreSQL bigint.

The function follows PostgreSQL's function calling convention, taking arguments through PG_FUNCTION_ARGS and returning a Datum. It handles edge cases carefully, including zero inputs and potential overflow conditions that could occur during the LCM calculation.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  -  (int64): First 64-bit integer argument extracted via PG_GETARG_INT64(0)
  -  (int64): Second 64-bit integer argument extracted via PG_GETARG_INT64(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (argument extraction)
  - PG_RETURN_INT64 (return value handling)  
  - [int8gcd_internal](int8gcd_internal.md) (GCD computation)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (overflow-safe multiplication)
  - PG_INT64_MIN (minimum value constant)
  - ereport/ERROR (error reporting)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Special case handling: lcm(x, 0) = lcm(0, x) = 0 to prevent division-by-zero and overflow errors
- Uses overflow-safe multiplication (pg_mul_s64_overflow) to detect arithmetic overflow
- Validates that result is not INT64_MIN, which cannot be properly represented 
- Always returns the absolute value of the computed LCM
- Part of PostgreSQL's bigint arithmetic functions in src/backend/utils/adt/int8.c:682-718