# floor_var

## Location
src/backend/utils/adt/numeric.c: 9985 - 10007

## Overview
The `floor_var` function returns the largest integer less than or equal to the given numeric value, implementing the mathematical floor function for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void floor_var(const NumericVar *var, NumericVar *result)
```

## Detailed Description
This function implements the floor operation for PostgreSQL's variable-precision numeric data type. It takes a `NumericVar` input and computes the largest integer that is less than or equal to the input value. The function works by first truncating the input to remove any fractional part, then subtracting 1 if the original number was negative and had a fractional component. The result is stored in the provided result parameter.

The algorithm:
1. Creates a temporary `NumericVar` and copies the input value
2. Truncates the temporary value to zero decimal places (removes fractional part)
3. If the original number was negative and not already an integer, subtracts 1 from the truncated value
4. Copies the final result to the output parameter

## Parameters / Member Variables
- `var`: Input `NumericVar` containing the numeric value to apply floor operation to
- `result`: Output `NumericVar` where the floor result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize a new `NumericVar` structure
  - `set_var_from_var`: Copy one `NumericVar` to another
  - `trunc_var`: Truncate a numeric value to specified decimal places
  - `cmp_var`: Compare two `NumericVar` values
  - `sub_var`: Subtract two `NumericVar` values
  - `free_var`: Free memory associated with a `NumericVar`
  - `NUMERIC_NEG`: Constant representing negative sign
  - `const_one`: Predefined `NumericVar` constant representing value 1

- Called from (representative examples):
  - `numeric_floor`: SQL-callable floor function wrapper
  - `compute_bucket`: Used in histogram bucket calculations

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- The function handles the sign correctly - positive numbers are truncated toward zero, negative numbers toward negative infinity
- Uses temporary variable management to avoid modifying the input parameter
- Part of PostgreSQL's high-precision arithmetic system that avoids floating-point limitations
- Complementary to `ceil_var` function with opposite rounding behavior for negative numbers