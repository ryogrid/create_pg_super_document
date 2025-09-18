# power_var

## Location
[src/backend/utils/adt/numeric.c:10947-11108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10947-L11108)

## Overview
Raises a base to the power of an exponent (base^exp) using logarithmic computation with intelligent optimization for integer exponents and comprehensive handling of edge cases.

## Definition
```c
static void power_var(const NumericVar *base, const NumericVar *exp, NumericVar *result)
```

## Detailed Description
This function implements exponentiation using the mathematical identity base^exp = e^(exp * ln(base)). The algorithm includes several optimizations and special case handling:

1. **Integer optimization**: If exp is an integer that fits in int32, delegates to power_var_int() for better performance
2. **Zero base handling**: Returns 0 immediately for 0^non-integer (0^0 is handled by power_var_int)
3. **Negative base validation**: Ensures exp is an integer when base is negative, determines result sign based on exp parity
4. **Precision management**: Uses estimate_ln_dweight() for overflow prevention and precision planning
5. **Logarithmic computation**: Computes result = e^(exp * ln(base)) using ln_var() and exp_var()

The function automatically determines appropriate scales for intermediate computations and the final result to ensure sufficient precision while preventing overflow.

## Parameters / Member Variables
- `base`: NumericVar representing the base number
- `exp`: NumericVar representing the exponent
- `result`: Output NumericVar to store the computed power

## Dependencies
- Functions called/Symbols referenced:
  - [numericvar_to_int64](../n/numericvar_to_int64.md) (for integer conversion)
  - [power_var_int](power_var_int.md) (for integer exponent optimization)
  - [estimate_ln_dweight](../e/estimate_ln_dweight.md) (for precision estimation)
  - [ln_var](../l/ln_var.md) (for natural logarithm computation)
  - [exp_var](../e/exp_var.md) (for exponential computation)
  - [cmp_var](../c/cmp_var.md) (for comparisons)
  - init_var, free_var, set_var_from_var (variable management)
  - [mul_var](../m/mul_var.md) (for multiplication)
  - [zero_var](../z/zero_var.md) (for zero assignment)
  - [numericvar_to_double_no_overflow](../n/numericvar_to_double_no_overflow.md) (for overflow testing)
- Constants used:
  - NUMERIC_POS, NUMERIC_NEG (sign indicators)
  - NUMERIC_MIN_SIG_DIGITS (minimum significant digits)
  - NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MAX_DISPLAY_SCALE (scale limits)
  - NUMERIC_MAX_RESULT_SCALE (overflow threshold)
  - PG_INT32_MIN, PG_INT32_MAX (integer limits)
- Called from:
  - [numeric_power](../n/numeric_power.md) (main power SQL function)

## Notes and Other Information
- This is a static function within the numeric.c module
- The function handles complex number avoidance by requiring integer exponents for negative bases
- Includes sophisticated overflow detection using approximate calculations before full computation
- The magic number 0.434294481903252 represents log10(e) used for decimal weight estimation
- Automatically determines result display scale rather than taking it as a parameter
- Implements SQL standard error codes for invalid operations
- Located at src/backend/utils/adt/numeric.c:10947-11108