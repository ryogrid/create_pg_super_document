# int4gcd_internal

## Location
src/backend/utils/adt/int.c: 1233 - 1293

## Overview
Internal implementation of the greatest common divisor (GCD) algorithm for 32-bit integers using the Euclidean algorithm with special handling for overflow cases.

## Definition
```c
static int32 int4gcd_internal(int32 arg1, int32 arg2)
```

## Detailed Description
The `int4gcd_internal` function computes the greatest common divisor of two 32-bit signed integers using the Euclidean algorithm. It implements several optimizations and special case handling:

1. **Input normalization**: Places the value with greater absolute value in `arg1` to minimize iterations
2. **Negative space arithmetic**: Performs comparisons in negative space to safely handle `INT_MIN`
3. **Overflow protection**: Special handling for `PG_INT32_MIN` cases that would cause overflow
4. **Edge case handling**: Properly handles cases like `gcd(x, 0) = abs(x)` and `gcd(0, 0) = 0`

The algorithm follows mathematical GCD properties where the result is always the largest positive integer that divides both inputs exactly.

## Parameters / Member Variables
- `arg1`: First 32-bit signed integer input
- `arg2`: Second 32-bit signed integer input

## Dependencies
- Functions called/Symbols referenced:
  - `PG_INT32_MIN`: Constant representing the minimum value for a 32-bit signed integer
  - `ereport()`: PostgreSQL error reporting function for overflow cases
- Called from (representative examples):
  - `int4gcd`: Public GCD function wrapper
  - `int4lcm`: Least common multiple function

## Notes and Other Information
- Static function, only accessible within the same compilation unit
- Uses Euclidean algorithm: repeatedly applies `gcd(a,b) = gcd(b, a mod b)` until one operand becomes zero
- Handles the mathematical edge case where `gcd(INT_MIN, -1)` would normally cause overflow by directly returning 1
- Performs initial swap to put larger absolute value in `arg1`, reducing the number of modulo operations needed
- Returns positive result by negating if the final result is negative
- Located in `src/backend/utils/adt/int.c` as part of PostgreSQL's integer arithmetic utilities