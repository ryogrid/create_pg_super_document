# cmp_abs

## Location
src/backend/utils/adt/numeric.c: 11522 - 11535

## Overview
Compares the absolute values of two NumericVar structures and returns an integer indicating their relative magnitude.

## Definition


## Detailed Description
This function compares the absolute values of two NumericVar structures, ignoring their signs. It serves as a high-level wrapper around the lower-level cmp_abs_common() function, extracting the necessary components (digits, ndigits, weight) from the NumericVar structures and delegating the actual comparison logic.

The function is part of PostgreSQL's lowest-level numeric operations that work on the variable level with unsigned arithmetic. It provides a clean interface for absolute value comparisons while the actual comparison logic is implemented in the more general cmp_abs_common() function.

## Parameters / Member Variables
- : Pointer to the first NumericVar to compare
- : Pointer to the second NumericVar to compare

## Dependencies
- Functions called/Symbols referenced:
  - cmp_abs_common (performs the actual comparison using digit arrays)
- Called from (representative examples):
  - add_var (addition operations need magnitude comparison)
  - sub_var (subtraction operations need magnitude comparison)
  - div_mod_var (division operations)
  - gcd_var (greatest common divisor calculations)
  - PGTYPESnumeric_add, PGTYPESnumeric_sub, PGTYPESnumeric_div (ECPG interface)
  - PGTYPESnumeric_cmp (ECPG comparison interface)

## Notes and Other Information
- Returns -1 if |var1| < |var2|, 0 if |var1| == |var2|, and 1 if |var1| > |var2|
- Static function internal to numeric.c, not exposed in the public API
- Part of the lowest-level unsigned arithmetic operations on NumericVar structures
- Commonly used in arithmetic operations where the sign is handled separately from magnitude
- Simple wrapper function that extracts NumericVar components and delegates to cmp_abs_common()
- Essential for implementing addition, subtraction, division, and other numeric operations that need magnitude comparisons