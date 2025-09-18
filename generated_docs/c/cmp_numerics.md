# cmp_numerics

## Location
src/backend/utils/adt/numeric.c: 2521 - 2577

## Overview
Core internal function that performs three-way comparison between two NUMERIC values, handling all special cases including NaN and infinity values.

## Definition
```c
static int cmp_numerics(Numeric num1, Numeric num2)
```

## Detailed Description
The `cmp_numerics` function is the central comparison engine for PostgreSQL's NUMERIC data type. It implements a comprehensive three-way comparison that returns negative, zero, or positive values to indicate whether the first numeric value is less than, equal to, or greater than the second value respectively.

The function handles several categories of numeric values in order of precedence:
1. **Special values**: NaN (Not a Number), positive infinity, and negative infinity
2. **Normal numeric values**: Standard numeric values with digits and precision

For special values, it establishes a consistent ordering: negative infinity < all normal values < positive infinity < NaN. NaN values are considered equal to each other and greater than all other values including positive infinity.

For normal numeric values, it delegates to `cmp_var_common` which performs the actual digit-by-digit comparison taking into account the sign, weight (scale), and individual digits of the numeric representation.

## Parameters / Member Variables
- `num1`: First NUMERIC value for comparison
- `num2`: Second NUMERIC value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_SPECIAL` (macro to check for special values)
  - `NUMERIC_IS_NAN` (macro to check for NaN)
  - `NUMERIC_IS_PINF` (macro to check for positive infinity)
  - `NUMERIC_IS_NINF` (macro to check for negative infinity)
  - [cmp_var_common](cmp_var_common.md) (performs comparison of normal numeric values)
  - `NUMERIC_DIGITS` (extracts digit array)
  - `NUMERIC_NDIGITS` (gets number of digits)
  - `NUMERIC_WEIGHT` (gets weight/scale)
  - `NUMERIC_SIGN` (gets sign)
- Called from:
  - `numeric_cmp` (main comparison function)
  - `numeric_eq`, `numeric_ne`, `numeric_gt`, `numeric_ge`, `numeric_lt`, `numeric_le` (comparison operators)
  - [numeric_smaller](../n/numeric_smaller.md), `numeric_larger` (min/max functions)
  - `width_bucket_numeric` (bucketing function)
  - [numeric_fast_cmp](../n/numeric_fast_cmp.md) (optimized comparison)

## Notes and Other Information
- This is a static (internal) function, not directly callable from SQL
- Implements a total ordering for all numeric values including special cases
- The ordering for special values is: NINF < normal values < PINF < NaN
- NaN values are considered equal to each other (returns 0)
- Return values: negative (num1 < num2), zero (num1 = num2), positive (num1 > num2)
- Located in `src/backend/utils/adt/numeric.c:2521-2577`
- Critical for the correctness of all numeric comparison operations in PostgreSQL
- Handles the complexity of special values so that higher-level comparison functions remain simple