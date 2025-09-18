# div_mod_var

## Location
src/backend/utils/adt/numeric.c: 9891 - 9960

## Overview
Calculates both the truncated integer quotient and numeric remainder from division of two PostgreSQL numeric variables in a single operation, ensuring mathematical consistency between the results.

## Definition
```c
static void div_mod_var(const NumericVar *var1, const NumericVar *var2,
                        NumericVar *quot, NumericVar *rem)
```

## Detailed Description
This function efficiently computes both the quotient and remainder of numeric division in one operation, maintaining mathematical consistency and proper sign handling. It uses an iterative refinement approach that starts with an initial estimate from div_var_fast() and then adjusts both quotient and remainder to satisfy the mathematical properties:

1. remainder has the same sign as the dividend (var1)
2. absolute value of remainder is less than absolute value of divisor (var2) 
3. dividend = quotient * divisor + remainder

The algorithm handles potential inaccuracies in the initial quotient estimate by iteratively adjusting both quotient and remainder until they satisfy the mathematical constraints. This ensures that both results are mathematically consistent and follow PostgreSQL's division semantics.

## Parameters / Member Variables
- `var1`: Dividend (first operand) in the division operation
- `var2`: Divisor (second operand) in the division operation
- `quot`: Output NumericVar to store the truncated integer quotient
- `rem`: Output NumericVar to store the remainder (precise to var2's dscale)

## Dependencies
- Functions called/Symbols referenced:
  - init_var (initialize temporary variables)
  - [div_var_fast](div_var_fast.md) (get initial quotient estimate)
  - [mul_var](../m/mul_var.md) (multiply for remainder calculation)
  - [sub_var](../s/sub_var.md), add_var (arithmetic operations for adjustments)
  - [cmp_abs](../c/cmp_abs.md) (absolute value comparison)
  - [set_var_from_var](../s/set_var_from_var.md) (copy results to output variables)
  - [free_var](../f/free_var.md) (deallocate temporary variables)
  - const_one (constant value for quotient adjustments)

- Called from (representative examples):
  - [sqrt_var](../s/sqrt_var.md) (square root implementation using Newton's method)

## Notes and Other Information
- Uses div_var_fast() for initial estimate, then refines for accuracy
- Iteratively adjusts quotient and remainder to satisfy mathematical constraints
- Ensures remainder has same sign as dividend and absolute value less than divisor
- Handles all sign combinations of dividend and divisor correctly
- More efficient than calling div_var() and mod_var() separately when both results are needed
- Critical for algorithms requiring both quotient and remainder with guaranteed mathematical consistency
- Supports Newton's method implementations and other advanced mathematical functions