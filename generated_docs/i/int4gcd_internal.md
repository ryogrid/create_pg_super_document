# int4gcd_internal

## Location
[src/backend/utils/adt/int.c:1233-1293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1233-L1293)

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
  - [int4gcd](int4gcd.md): Public GCD function wrapper
  - [int4lcm](int4lcm.md): Least common multiple function

## Notes and Other Information
- Static function, only accessible within the same compilation unit
- Uses Euclidean algorithm: repeatedly applies `gcd(a,b) = gcd(b, a mod b)` until one operand becomes zero
- Handles the mathematical edge case where `gcd(INT_MIN, -1)` would normally cause overflow by directly returning 1
- Performs initial swap to put larger absolute value in `arg1`, reducing the number of modulo operations needed
- Returns positive result by negating if the final result is negative
- Located in `src/backend/utils/adt/int.c` as part of PostgreSQL's integer arithmetic utilities

## Simplified Source

```c
static int32 int4gcd_internal(int32 a, int32 b) {
    // Normalize inputs: put greater absolute value in 'b'
    // Work in negative space to handle INT_MIN safely
    int32 neg_a = (a < 0) ? a : -a;
    int32 neg_b = (b < 0) ? b : -b;
    if (neg_a > neg_b) {
        int32 temp = a; a = b; b = temp;  // swap
    }

    // Handle INT_MIN overflow cases
    if (a == PG_INT32_MIN) {
        if (b == 0 || b == PG_INT32_MIN) {
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("integer out of range")));
        }
        if (b == -1) return 1;  // gcd(INT_MIN, -1) = 1
    }

    // Euclidean algorithm: gcd(a,b) = gcd(b, a % b)
    while (b != 0) {
        int32 temp = b;
        b = a % b;
        a = temp;
    }

    // Ensure positive result
    return (a < 0) ? -a : a;
}
```