# sub_var

## Location
src/backend/utils/adt/numeric.c: 8564 - 8684

## Overview
Performs subtraction of two NumericVar values, handling signs and delegating to appropriate absolute value operations.

## Definition


## Detailed Description
The  function is the full version of subtraction functionality at the variable level for PostgreSQL's NUMERIC data type. It handles signed subtraction by analyzing the signs of both operands and determining the appropriate operation (addition or subtraction of absolute values). The function safely handles cases where the result might point to one of the operands without causing memory issues.

The function implements the mathematical rules for signed number subtraction:
- (+A) - (-B) = +(A + B)
- (+A) - (+B) = +(A - B) if A > B, -(B - A) if B > A, 0 if A = B
- (-A) - (-B) = -(A - B) if A > B, +(B - A) if B > A, 0 if A = B
- (-A) - (+B) = -(A + B)

## Parameters / Member Variables
- : First NumericVar operand (minuend, input)
- : Second NumericVar operand (subtrahend, input)
- : NumericVar to store the subtraction result (output)

## Dependencies
- Functions called/Symbols referenced:
  - add_abs (for adding absolute values)
  - sub_abs (for subtracting absolute values)
  - cmp_abs (for comparing absolute values)
  - zero_var (for setting result to zero)
  - NUMERIC_POS (positive sign constant)
  - NUMERIC_NEG (negative sign constant)
- Called from (representative examples):
  - numeric_sub_opt_error
  - compute_bucket
  - in_range_numeric_numeric
  - numeric_stddev_internal
  - mod_var
  - div_mod_var
  - floor_var
  - sqrt_var
  - ln_var

## Notes and Other Information
- This is a static function internal to the numeric.c module
- The function is safe for in-place operations where result points to one of the input operands
- The decimal scale (dscale) of the result is set to the maximum of the input scales when the result is zero
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Complementary to add_var, implementing the inverse operation