# power_var_int

## Location
[src/backend/utils/adt/numeric.c:11109-11313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11109-L11313)

## Overview
Raises a base to an integer power (base^exp) using efficient exponentiation by squaring algorithm with dynamic precision management and comprehensive special case handling.

## Definition
```c
static void power_var_int(const NumericVar *base, int exp, int exp_dscale, NumericVar *result)
```

## Detailed Description
This function implements integer exponentiation using the binary exponentiation (exponentiation by squaring) algorithm, which is significantly more efficient than the logarithmic approach used in power_var(). The implementation includes:

1. **Precision estimation**: Uses double precision approximation to estimate result weight and prevent overflow/underflow
2. **Special case optimization**: Handles common cases (exp = 0, 1, -1, 2) with direct computation
3. **Zero base handling**: Properly handles 0^exp cases including the 0^0 = 1 convention
4. **Binary exponentiation**: Uses bit manipulation to efficiently compute powers through repeated squaring
5. **Dynamic scaling**: Adjusts local rscale during computation to maintain required precision
6. **Overflow protection**: Monitors intermediate results to detect overflow early

The algorithm processes the binary representation of the exponent, squaring the base at each bit position and multiplying the result when the bit is set.

## Parameters / Member Variables
- `base`: NumericVar representing the base number
- `exp`: Integer exponent value
- `exp_dscale`: Display scale of the original exponent (for result scale determination)
- `result`: Output NumericVar to store the computed power

## Dependencies
- Functions called/Symbols referenced:
  - [set_var_from_var](../s/set_var_from_var.md) (for variable copying)
  - [round_var](../r/round_var.md) (for result rounding)
  - [div_var](../d/div_var.md), div_var_fast (for division operations)
  - [mul_var](../m/mul_var.md) (for multiplication)
  - [zero_var](../z/zero_var.md) (for zero assignment)
  - init_var, free_var (variable lifecycle management)
  - log10, log, fabs (standard math functions)
- Constants used:
  - const_one (numeric constant 1)
  - DEC_DIGITS, NBASE (numeric system constants)
  - NUMERIC_WEIGHT_MAX (maximum weight limit)
  - NUMERIC_MIN_SIG_DIGITS (minimum significant digits)
  - NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MAX_DISPLAY_SCALE (scale limits)
- Called from:
  - [power_var](power_var.md) (for integer exponent optimization)

## Notes and Other Information
- This is a static function within the numeric.c module
- Implements SQL:2003 standard for 0^0 = 1
- Uses binary exponentiation for O(log n) complexity instead of O(n)
- Includes sophisticated overflow detection to prevent excessive computation
- The algorithm dynamically adjusts precision during computation to balance accuracy and performance
- Handles both positive and negative exponents efficiently
- The sig_digits calculation includes error estimation based on log10(abs(exp)) to account for accumulated multiplication errors
- Located at src/backend/utils/adt/numeric.c:11109-11313