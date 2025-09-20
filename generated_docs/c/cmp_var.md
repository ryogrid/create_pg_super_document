# cmp_var

## Location
[src/backend/utils/adt/numeric.c:8389-8403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8389-L8403)

## Overview
Compares two NumericVar values and returns an integer indicating their relative ordering.

## Definition

```c
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
```
## Detailed Description
This function provides a high-level interface for comparing two NumericVar structures. It extracts the relevant fields (digits, ndigits, weight, and sign) from both input variables and delegates the actual comparison logic to the lower-level cmp_var_common function. The function assumes that both input variables have been properly normalized with leading and trailing zeros removed. The comparison follows standard numeric ordering rules, taking into account both magnitude and sign.

This is a convenience wrapper that simplifies the interface for comparing complete NumericVar structures by automatically extracting the necessary components for comparison.

## Parameters / Member Variables
- : Pointer to the first NumericVar structure to compare
- : Pointer to the second NumericVar structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_var_common](cmp_var_common.md): Core comparison function that performs the actual comparison logic

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization checking
  - generate_series_step_numeric: Series generation with numeric steps
  - [compute_bucket](compute_bucket.md): Bucketing operations for histograms
  - [in_range_numeric_numeric](../i/in_range_numeric_numeric.md): Range checking for numeric values
  - [numeric_power](../n/numeric_power.md): Power function calculations
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md): Standard deviation computations
  - [ceil_var](ceil_var.md): Ceiling function implementation
  - [floor_var](../f/floor_var.md): Floor function implementation
  - [sqrt_var](../s/sqrt_var.md): Square root calculations
  - [estimate_ln_dweight](../e/estimate_ln_dweight.md): Natural logarithm weight estimation
  - [ln_var](../l/ln_var.md): Natural logarithm calculations
  - [power_var](../p/power_var.md): Power function variable operations
  - [random_var](../r/random_var.md): Random number generation

## Notes and Other Information
- Returns negative value if var1 < var2, zero if var1 == var2, positive if var1 > var2
- Assumes input variables have been stripped of leading/trailing zeros
- Serves as a convenient wrapper around the more complex cmp_var_common function
- Used extensively throughout numeric mathematical operations that require ordering comparisons
- Essential for implementing comparison operators (<, <=, =, >=, >) for the numeric data type