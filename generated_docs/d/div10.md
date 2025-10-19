# div10

## Location
[src/common/d2s_intrinsics.h:157-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s_intrinsics.h#L157-L162)

## Overview
Performs efficient division by 10 using multiplication and bit shifting instead of traditional division operations, optimized for 32-bit platforms where compilers typically generate library function calls for 64-bit divisions.

## Definition
```c
static inline uint64
div10(const uint64 x)
```

## Detailed Description
The `div10` function implements division-by-constant optimization using the multiply-high technique. Similar to `div5`, it uses a magic multiplier constant (0xCCCCCCCCCCCCCCCD) but with a different shift amount (3 bits instead of 2) to compute floor(x/10) efficiently.

This optimization avoids expensive division operations that would typically result in library function calls on 32-bit platforms. The magic constant and shift values are specifically chosen to produce mathematically correct results for division by 10 across the full range of 64-bit unsigned integers.

Division by 10 is particularly important in decimal number formatting and string conversion routines, making this optimization valuable for performance-critical code paths.

## Parameters / Member Variables
- `x`: The 64-bit unsigned integer dividend to be divided by 10

## Dependencies
- Functions called/Symbols referenced:
  - [umulh](../u/umulh.md) (returns the high 64 bits of 128-bit multiplication)
- Called from (representative examples):
  - [d2d](d2d.md) (multiple calls in src/common/d2s.c at lines 506, 507, 513, 529, 535, 536, 598, 599, 604)
  - [to_chars](../t/to_chars.md) (in src/common/d2s.c:829)

## Notes and Other Information
- This function is part of the Ryu algorithm implementation for fast floating-point to string conversion
- Uses the same magic multiplier as `div5` but with a different shift amount (3 vs 2 bits)
- Critical for decimal digit extraction in number-to-string conversion routines
- The implementation is marked as `static inline` for performance optimization
- Extensively used in the d2d function for decimal formatting operations
- Related to other division optimization functions: `div5`, `div100`, and `div1e8`

## Simplified Source

```c
static inline uint64 div10(const uint64 x) {
    // Efficient division by 10 using bit manipulation
    // Magic constant 0xCCCCCCCCCCCCCCCD represents 1/10 in fixed-point arithmetic
    uint64 high_bits = umulh(x, 0xCCCCCCCCCCCCCCCD);

    // Shift by 3 bits to complete the division
    // Same magic constant as div5 but different shift (3 vs 2 bits)
    return high_bits >> 3;
}
```