# sub_var

## Location
[src/backend/utils/adt/numeric.c:8564-8684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8564-L8684)

## Overview
Performs subtraction of two NumericVar values, handling signs and delegating to appropriate absolute value operations.

## Definition

```c
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```
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
  - [add_abs](../a/add_abs.md) (for adding absolute values)
  - [sub_abs](sub_abs.md) (for subtracting absolute values)
  - [cmp_abs](../c/cmp_abs.md) (for comparing absolute values)
  - [zero_var](../z/zero_var.md) (for setting result to zero)
  - NUMERIC_POS (positive sign constant)
  - NUMERIC_NEG (negative sign constant)
- Called from (representative examples):
  - [numeric_sub_opt_error](../n/numeric_sub_opt_error.md)
  - [compute_bucket](../c/compute_bucket.md)
  - [in_range_numeric_numeric](../i/in_range_numeric_numeric.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)
  - [mod_var](../m/mod_var.md)
  - [div_mod_var](../d/div_mod_var.md)
  - [floor_var](../f/floor_var.md)
  - [sqrt_var](sqrt_var.md)
  - [ln_var](../l/ln_var.md)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- The function is safe for in-place operations where result points to one of the input operands
- The decimal scale (dscale) of the result is set to the maximum of the input scales when the result is zero
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Complementary to add_var, implementing the inverse operation