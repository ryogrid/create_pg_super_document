# cmp_var

## Location
src/backend/utils/adt/numeric.c: 8389 - 8403

## Overview
Compares two NumericVar values and returns an integer indicating their relative ordering.

## Definition


## Detailed Description
This function provides a high-level interface for comparing two NumericVar structures. It extracts the relevant fields (digits, ndigits, weight, and sign) from both input variables and delegates the actual comparison logic to the lower-level cmp_var_common function. The function assumes that both input variables have been properly normalized with leading and trailing zeros removed. The comparison follows standard numeric ordering rules, taking into account both magnitude and sign.

This is a convenience wrapper that simplifies the interface for comparing complete NumericVar structures by automatically extracting the necessary components for comparison.

## Parameters / Member Variables
- : Pointer to the first NumericVar structure to compare
- : Pointer to the second NumericVar structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - cmp_var_common: Core comparison function that performs the actual comparison logic

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization checking
  - generate_series_step_numeric: Series generation with numeric steps
  - compute_bucket: Bucketing operations for histograms
  - in_range_numeric_numeric: Range checking for numeric values
  - numeric_power: Power function calculations
  - numeric_stddev_internal: Standard deviation computations
  - ceil_var: Ceiling function implementation
  - floor_var: Floor function implementation
  - sqrt_var: Square root calculations
  - estimate_ln_dweight: Natural logarithm weight estimation
  - ln_var: Natural logarithm calculations
  - power_var: Power function variable operations
  - random_var: Random number generation

## Notes and Other Information
- Returns negative value if var1 < var2, zero if var1 == var2, positive if var1 > var2
- Assumes input variables have been stripped of leading/trailing zeros
- Serves as a convenient wrapper around the more complex cmp_var_common function
- Used extensively throughout numeric mathematical operations that require ordering comparisons
- Essential for implementing comparison operators (<, <=, =, >=, >) for the numeric data type