# clamp_row_est

## Location
[src/backend/optimizer/path/costsize.c:202-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L202-L230)

## Overview
Forces a row-count estimate to a sane value by clamping it to a reasonable range and ensuring it's a valid numeric value.

## Definition

```c
double
clamp_row_est(double nrows)
```
## Detailed Description
This function sanitizes row count estimates to prevent problematic values from propagating through the PostgreSQL query optimizer's cost calculations. It handles several edge cases:

1. **Infinite/NaN values**: Replaced with MAXIMUM_ROWCOUNT to avoid useless cost calculations
2. **Zero or negative values**: Set to 1.0 to ensure explain output looks reasonable and prevent divide-by-zero errors during cost interpolation
3. **Fractional values**: Rounded to the nearest integer using rint() for cleaner estimates

The function is essential for maintaining numerical stability in the cost-based optimizer and ensuring that cost calculations remain meaningful even when statistics are incomplete or unusual.

## Parameters / Member Variables
- : The input row count estimate (double precision floating point number)

## Dependencies
- Functions called/Symbols referenced:
  - : C standard library function to check for NaN values
  - : PostgreSQL constant defining the maximum allowed row count
  - : C math library function for rounding to nearest integer

- Called from (representative examples):
  - : Sequential scan cost estimation
  - : Index scan cost estimation
  - : Bitmap heap scan cost estimation
  - : Nested loop join cost finalization
  - : Base relation size estimation

## Notes and Other Information
- Located in src/backend/optimizer/path/costsize.c:202-230
- This is a utility function widely used throughout the query optimizer
- The minimum return value is always 1.0, never zero
- All returned values are integers (achieved through rint())
- Critical for preventing numerical instabilities in cost calculations
- Used in both planning and execution phases where row count estimates are needed