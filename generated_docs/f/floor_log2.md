# floor_log2

## Location
[src/backend/utils/adt/array_selfuncs.c:1089-1129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L1089-L1129)

## Overview
Fast calculation of the floor value of base-2 logarithm for 32-bit unsigned integers using bit manipulation.

## Definition
```c
static int floor_log2(uint32 n)
```

## Detailed Description
This function efficiently computes floor(log₂(n)) using a binary search approach with bit shifting operations. It progressively narrows down the position of the most significant bit by testing powers of 2 in decreasing order (16, 8, 4, 2, 1). The algorithm avoids expensive floating-point logarithm calculations by using only integer operations.

The function returns the position of the highest set bit, which equals floor(log₂(n)). For example:
- floor_log2(1) = 0 (since 2⁰ = 1)
- floor_log2(7) = 2 (since 2² ≤ 7 < 2³)
- floor_log2(8) = 3 (since 2³ = 8)

## Parameters / Member Variables
- `n`: 32-bit unsigned integer to compute the floor log₂ for

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic bit operations)
- Called from (representative examples):
  - [mcelem_array_contain_overlap_selec](../m/mcelem_array_contain_overlap_selec.md)
  - Functions referenced via DEFAULT_SEL

## Notes and Other Information
- Returns -1 for input value 0 (since log₂(0) is undefined)
- Uses bit shifting optimization: progressively tests if n >= 2^k for k = 16, 8, 4, 2, 1
- Time complexity: O(1) with exactly 5 conditional checks maximum
- Space complexity: O(1)
- Used in array selectivity estimation to determine optimal search algorithms (binary vs linear)
- Common usage pattern: deciding between binary search and linear scan based on array sizes
- More efficient than using mathematical log functions for integer inputs

## Simplified Source

```c
static int floor_log2(uint32 n) {
    int result = 0;

    // Handle edge case: log2(0) is undefined
    if (n == 0) return -1;

    // Binary search for highest set bit position
    // Check each power of 2 in decreasing order
    if (n >= (1 << 16)) { n >>= 16; result += 16; }  // Check 2^16
    if (n >= (1 << 8))  { n >>= 8;  result += 8;  }  // Check 2^8
    if (n >= (1 << 4))  { n >>= 4;  result += 4;  }  // Check 2^4
    if (n >= (1 << 2))  { n >>= 2;  result += 2;  }  // Check 2^2
    if (n >= (1 << 1))  { result += 1; }             // Check 2^1

    return result;  // Position of highest set bit = floor(log2(n))
}
```