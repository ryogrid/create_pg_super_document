# mod_var

## Location
src/backend/utils/adt/numeric.c: 9862 - 9890

## Overview
Calculates the modulo (remainder) of two PostgreSQL numeric variables using the mathematical relationship mod(x,y) = x - trunc(x/y)*y.

## Definition
```c
static void mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```

## Detailed Description
This function implements modulo operation for PostgreSQL numeric variables by employing the mathematical identity mod(x,y) = x - trunc(x/y)*y. Rather than implementing modulo directly, it leverages existing division, multiplication, and subtraction functions to compute the remainder.

The algorithm works by:
1. Computing trunc(x/y) using div_var with scale 0 and no rounding (truncation)
2. Multiplying the truncated quotient by the divisor y
3. Subtracting this product from the original dividend x to get the remainder

This approach ensures consistency with PostgreSQL's division behavior and handles all edge cases (including negative numbers) according to the standard mathematical definition of modulo.

## Parameters / Member Variables
- `var1`: Dividend (first operand) in the modulo operation
- `var2`: Divisor (second operand) in the modulo operation  
- `result`: Output NumericVar to store the modulo result

## Dependencies
- Functions called/Symbols referenced:
  - init_var (initialize temporary variable)
  - [div_var](../d/div_var.md) (perform division with truncation)
  - [mul_var](mul_var.md) (multiply quotient by divisor)
  - [sub_var](../s/sub_var.md) (subtract to get remainder)
  - [free_var](../f/free_var.md) (deallocate temporary variable memory)

- Called from (representative examples):
  - [numeric_mod_opt_error](../n/numeric_mod_opt_error.md) (modulo operation with error handling)
  - [gcd_var](../g/gcd_var.md) (greatest common divisor calculation)

## Notes and Other Information
- Uses mathematical identity rather than direct modulo implementation for consistency
- Handles negative numbers according to standard mathematical modulo definition
- Inherits division-by-zero protection from the underlying div_var function
- [Result](../R/Result.md) scale matches the divisor's display scale for consistency
- Employs temporary variable management to avoid memory leaks
- Provides foundation for higher-level modulo operations and mathematical functions like GCD