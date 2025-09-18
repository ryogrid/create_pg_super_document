# floor_log2

## Location
src/backend/utils/adt/array_selfuncs.c: 1089 - 1129

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