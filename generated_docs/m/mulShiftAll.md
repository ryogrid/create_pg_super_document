# mulShiftAll

## Location
[src/common/d2s.c:208-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L208-L220)

## Overview
Computes three related mulShift operations simultaneously for floating-point boundary calculations in the Ryu algorithm.

## Definition
```c
static inline uint64 mulShiftAll(const uint64 m, const uint64 *const mul, const int32 j,
                                 uint64 *const vp, uint64 *const vm, const uint32 mmShift)
```

## Detailed Description
This function performs three mulShift operations at once, computing the main value and its upper and lower boundaries needed for the Ryu floating-point to string conversion algorithm. These three values represent different precision boundaries around the floating-point number being converted.

The function computes:
1. Upper boundary (*vp): mulShift(4*m + 2, mul, j) - represents the upper acceptable boundary
2. Lower boundary (*vm): mulShift(4*m - 1 - mmShift, mul, j) - represents the lower acceptable boundary  
3. Main value (return): mulShift(4*m, mul, j) - represents the main converted value

The scaling by 4 and the additions/subtractions (±2, -1-mmShift) are part of the Ryu algorithm's boundary calculations that ensure the converted string representation is the shortest and most accurate possible. The mmShift parameter adjusts the lower boundary calculation based on specific floating-point characteristics.

## Parameters / Member Variables
- `m`: The 64-bit mantissa value to be processed
- `mul`: Pointer to a 2-element array representing a 128-bit multiplier
- `j`: The number of bits to shift the results right by
- `vp`: Output parameter for the upper boundary value (4*m + 2 case)  
- `vm`: Output parameter for the lower boundary value (4*m - 1 - mmShift case)
- `mmShift`: Adjustment value for the lower boundary calculation

## Dependencies
- Functions called/Symbols referenced:
  - [mulShift](mulShift.md) (called three times with different parameters)
- Called from (representative examples):
  - [d2d](../d/d2d.md) (multiple calls in src/common/d2s.c)

## Notes and Other Information
- This function is marked as `static inline` for performance optimization
- Part of the Ryu algorithm implementation for double-precision floating-point to string conversion
- Efficiently computes three boundary values in a single function call
- The scaling factors (4*m variations) and adjustments are mathematically derived from the Ryu algorithm
- Used in the main d2d (double-to-decimal) conversion function
- The mmShift parameter handles special cases in floating-point boundary calculations
- Returns the main conversion value while storing the boundaries in the provided output parameters

## Simplified Source

```c
static inline uint64 mulShiftAll(const uint64 m, const uint64 *const mul, const int32 j,
                                 uint64 *const vp, uint64 *const vm, const uint32 mmShift) {
    // Compute three boundary values for Ryu algorithm floating-point conversion

    // Upper boundary: 4*m + 2
    *vp = mulShift(4 * m + 2, mul, j);

    // Lower boundary: 4*m - 1 - mmShift
    *vm = mulShift(4 * m - 1 - mmShift, mul, j);

    // Main value: 4*m
    return mulShift(4 * m, mul, j);
}
```