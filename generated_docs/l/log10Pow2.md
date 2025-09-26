# log10Pow2

## Location
[src/common/ryu_common.h:70-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/ryu_common.h#L70-L82)

## Overview
Returns the floor of the base-10 logarithm of 2^e, specifically computing floor(log_10(2^e)).

## Definition
```c
static inline int32 log10Pow2(const int32 e)
```

## Detailed Description
This function calculates the largest integer less than or equal to log base 10 of 2 raised to the power e. It uses an efficient approximation formula: (int32) ((((uint32) e) * 78913) >> 18). This avoids expensive logarithmic computations by using integer arithmetic with a precomputed magic constant (78913) that represents an approximation of e * log_10(2).

The function includes safety assertions to ensure the input parameter e is within valid bounds (0 <= e <= 1650). The approximation fails for values beyond e = 1651 (corresponding to 2^1651, which is just greater than 10^297).

## Parameters / Member Variables
- `e`: The exponent value (int32). Must be non-negative and <= 1650 to ensure the approximation remains accurate.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for input validation)
- Called from (representative examples):
  - [d2d](../d/d2d.md) (in src/common/d2s.c at line 396)
  - [f2d](../f/f2d.md) (in src/common/f2s.c at line 265)

## Notes and Other Information
- The approximation is valid up to e = 1650; beyond that, it fails for 2^1651 > 10^297
- The magic constant 78913 and right shift by 18 bits efficiently compute floor(e * log_10(2))
- This function is part of the Ryu algorithm for fast floating-point to string conversion
- The mathematical basis relies on log_10(2) ≈ 0.30103, and the constant 78913/262144 ≈ 0.30103
- Located in src/common/ryu_common.h:70-82