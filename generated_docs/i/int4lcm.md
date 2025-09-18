# int4lcm

## Location
src/backend/utils/adt/int.c: 1309 - 1345

## Overview
PostgreSQL SQL-callable function that computes the least common multiple (LCM) of two 32-bit integers with overflow protection.

## Definition
```c
Datum int4lcm(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4lcm` function computes the least common multiple of two 32-bit signed integers using the mathematical relationship `lcm(x, y) = abs(x / gcd(x, y) * y)`. The function includes comprehensive overflow detection and special case handling:

1. **Zero handling**: `lcm(x, 0) = lcm(0, x) = 0` as a special case
2. **GCD computation**: Uses `int4gcd_internal` to find the greatest common divisor
3. **Overflow detection**: Uses `pg_mul_s32_overflow` to detect multiplication overflow
4. **Range validation**: Ensures the result can be represented as a positive 32-bit integer

The function follows the mathematical definition where LCM is the smallest positive integer that is divisible by both input values.

## Parameters / Member Variables
- First parameter (accessed via `PG_GETARG_INT32(0)`): First 32-bit integer input
- Second parameter (accessed via `PG_GETARG_INT32(1)`): Second 32-bit integer input

## Dependencies
- Functions called/Symbols referenced:
  - `[int4gcd_internal](int4gcd_internal.md)`: Internal GCD implementation used in LCM calculation
  - `[pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md)`: PostgreSQL function to detect 32-bit signed integer multiplication overflow
  - `PG_INT32_MIN`: Constant representing the minimum value for a 32-bit signed integer
  - `PG_GETARG_INT32()`: PostgreSQL macro to extract int32 arguments
  - `PG_RETURN_INT32()`: PostgreSQL macro to return int32 result
  - `ereport()`: PostgreSQL error reporting function
- Called from (representative examples):
  - No direct references found in the codebase (typically called from SQL)

## Notes and Other Information
- Implements the mathematical formula: `lcm(x, y) = |x * y| / gcd(x, y)`, rearranged as `|x / gcd(x, y) * y|` to minimize intermediate overflow risk
- Special handling prevents division-by-zero when one argument is zero and overflow when `INT_MIN` is involved
- Uses PostgreSQL's standard function calling convention with `PG_FUNCTION_ARGS`
- Can be invoked from SQL as a built-in function for LCM computation
- Always returns a positive result by taking the absolute value
- Part of PostgreSQL's integer arithmetic functions in `src/backend/utils/adt/int.c`
- The `unlikely()` macro is used for branch prediction optimization on overflow checks