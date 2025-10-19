# int8gcd_internal

## Location
[src/backend/utils/adt/int8.c:606-666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L606-L666)

## Overview
Internal implementation of the Greatest Common Divisor (GCD) algorithm for 64-bit signed integers using the Euclidean algorithm with special handling for edge cases.

## Definition

```c
static int64
int8gcd_internal(int64 arg1, int64 arg2)
```
## Detailed Description
The int8gcd_internal function implements the mathematical greatest common divisor operation using the Euclidean algorithm. It finds the largest positive integer that exactly divides both input values. The function includes comprehensive handling for special cases including zero inputs and the problematic INT64_MIN value. It performs computations in negative space initially to handle INT64_MIN safely, then converts the final result to positive. The algorithm optimizes by ensuring the larger absolute value is processed first to reduce the number of modulo operations needed.

## Parameters / Member Variables
- : First 64-bit signed integer input
- : Second 64-bit signed integer input
- : Temporary variable for swapping values during algorithm execution
- : Negative representation of arg1's absolute value for safe INT64_MIN handling
- : Negative representation of arg2's absolute value for safe INT64_MIN handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT64_MIN (minimum int64 constant for overflow detection)
  - ereport (error reporting for overflow cases)
  - [errcode](../e/errcode.md)/errmsg (error code and message macros)
- Called from:
  - [int8gcd](int8gcd.md) (public GCD function wrapper)
  - [int8lcm](int8lcm.md) (least common multiple function)

## Notes and Other Information
- Implements mathematical properties: gcd(x, 0) = gcd(0, x) = abs(x), gcd(0, 0) = 0
- Special case handling for INT64_MIN prevents overflow when computing absolute values
- Uses negative space computation to safely handle INT64_MIN without overflow
- Optimizes by putting larger absolute value in arg1 first to reduce iterations
- Guards against floating-point exceptions on some hardware for INT64_MIN % -1
- Returns positive result always (except for gcd(0,0) = 0)
- Core algorithm for both GCD and LCM operations in PostgreSQL bigint arithmetic

## Simplified Source

```c
static int64
int8gcd_internal(int64 arg1, int64 arg2)
{
    int64 swap;

    // Handle larger absolute value in arg1 (work in negative space for INT64_MIN safety)
    int64 a1 = (arg1 < 0) ? arg1 : -arg1;
    int64 a2 = (arg2 < 0) ? arg2 : -arg2;
    if (a1 > a2) {
        swap = arg1; arg1 = arg2; arg2 = swap;
    }

    // Special handling for INT64_MIN overflow cases
    if (arg1 == PG_INT64_MIN) {
        if (arg2 == 0 || arg2 == PG_INT64_MIN)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("bigint out of range")));
        if (arg2 == -1)
            return 1;  // gcd(INT64_MIN, -1) = 1
    }

    // Euclidean algorithm: repeatedly divide and take remainder
    while (arg2 != 0) {
        swap = arg2;
        arg2 = arg1 % arg2;
        arg1 = swap;
    }

    // Ensure positive result
    return (arg1 < 0) ? -arg1 : arg1;
}
```