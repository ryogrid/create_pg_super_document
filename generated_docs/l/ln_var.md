# ln_var

## Location
src/backend/utils/adt/numeric.c: 10769 - 10886

## Overview
Computes the natural logarithm of a numeric value using Taylor series expansion with input range reduction through repeated square root operations.

## Definition
```c
static void ln_var(const NumericVar *arg, NumericVar *result, int rscale)
```

## Detailed Description
This function implements natural logarithm calculation through a sophisticated algorithm:

1. **Input validation**: Checks for zero and negative inputs, throwing appropriate errors
2. **Range reduction**: Uses repeated sqrt() operations to reduce input into the range 0.9 < x < 1.1
3. **Taylor series computation**: Applies the Taylor series for 0.5 * ln((1+z)/(1-z)) where z = (x-1)/(x+1)
4. **Result compensation**: Multiplies the result by 2^(nsqrt+1) to compensate for the range reduction

The algorithm uses the Taylor series: z + z^3/3 + z^5/5 + ... where z is in the approximate range -0.053 to 0.048 due to range reduction. This ensures good convergence properties while maintaining numerical precision.

## Parameters / Member Variables
- `arg`: Input NumericVar for which to compute the natural logarithm
- `result`: Output NumericVar to store the computed logarithm
- `rscale`: Desired scale (number of decimal places) for the result

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_var](../c/cmp_var.md) (for comparisons with constants)
  - init_var, free_var (variable lifecycle management)
  - [set_var_from_var](../s/set_var_from_var.md) (variable copying)
  - [sqrt_var](../s/sqrt_var.md) (square root computation)
  - [mul_var](../m/mul_var.md), add_var, sub_var, div_var_fast, div_var_int (arithmetic operations)
- Constants used:
  - const_zero, const_one, const_two (numeric constants)
  - const_zero_point_nine, const_one_point_one (range boundaries)
  - DEC_DIGITS (decimal digits constant)
- Called from:
  - [numeric_ln](../n/numeric_ln.md) (main natural logarithm SQL function)
  - [log_var](log_var.md) (logarithm with arbitrary base)
  - [power_var](../p/power_var.md) (power function implementation)

## Notes and Other Information
- This is a static function within the numeric.c module
- The function uses dynamic precision adjustment during range reduction to optimize performance
- The magic number 0.301029995663981 represents log10(2) used for precision calculations
- Error handling includes specific error codes for invalid logarithm arguments
- The algorithm balances numerical stability with computational efficiency
- Located at src/backend/utils/adt/numeric.c:10769-10886