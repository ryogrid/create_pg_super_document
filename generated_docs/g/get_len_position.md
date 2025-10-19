# get_len_position

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:873-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L873-L917)

## Overview
Calculates the relative position of a length value within a length histogram bin, returning a normalized position in the range [0,1] for use in length-based selectivity estimation.

## Definition
```c
static double get_len_position(double value, double hist1, double hist2)
```

## Detailed Description
This function determines where a specific length value lies within a histogram bin bounded by `hist1` and `hist2` length values. Unlike `get_position()` which works with range bounds and requires subdiff functions, this function operates directly on numeric length values using simple arithmetic.

The function handles several cases:
1. **Finite bounds**: Uses linear interpolation formula: `1.0 - (hist2 - value) / (hist2 - hist1)`
2. **Infinite lower bound**: Returns 1.0 indicating the value is far from the infinite lower bound
3. **Infinite upper bound**: Returns 0.0 indicating the value is close to the finite lower bound  
4. **Both bounds infinite**: Returns 0.5 as a middle position fallback

This is specifically designed for length histogram bins where values represent range lengths as floating-point numbers.

## Parameters / Member Variables
- `value`: Target length value whose position within the bin needs to be determined
- `hist1`: Lower boundary of the length histogram bin
- `hist2`: Upper boundary of the length histogram bin

## Dependencies
- Functions called/Symbols referenced:
  - isinf
- Called from (representative examples):
  - [calc_length_hist_frac](../c/calc_length_hist_frac.md)

## Notes and Other Information
- Returns 0.5 as a fallback when the value is infinite but bounds are finite (defensive programming)
- Uses direct arithmetic rather than type-specific subdiff functions since it operates on numeric lengths
- The linear interpolation formula ensures accurate positioning within finite numeric ranges
- Complements the range bound positioning done by `get_position()` for complete selectivity estimation
- Essential for PostgreSQL query planner's cost estimation when dealing with range length-based predicates

## Simplified Source

```c
static double get_len_position(double value, double hist1, double hist2) {
    // Both bounds are finite - use linear interpolation
    if (!isinf(hist1) && !isinf(hist2)) {
        // Safety check: if value is infinite between finite bounds, return middle
        if (isinf(value))
            return 0.5;

        // Linear interpolation: calculate relative position in bin
        return 1.0 - (hist2 - value) / (hist2 - hist1);
    }
    // Lower bound infinite, upper finite - value is far from lower bound
    else if (isinf(hist1) && !isinf(hist2)) {
        return 1.0;
    }
    // Upper bound infinite, lower finite - value is close to lower bound
    else if (!isinf(hist1) && isinf(hist2)) {
        return 0.0;
    }
    // Both bounds infinite - assume middle position
    else {
        return 0.5;
    }
}
```