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

## Simplified Source

```c
static void
power_var(const NumericVar *base, const NumericVar *exp, NumericVar *result)
{
    NumericVar abs_base, ln_base, ln_num;

    // Optimization: use integer algorithm if exponent is a small integer
    if (exp->ndigits == 0 || exp->ndigits <= exp->weight + 1) {
        int64 expval64;
        if (numericvar_to_int64(exp, &expval64) &&
            expval64 >= PG_INT32_MIN && expval64 <= PG_INT32_MAX) {
            power_var_int(base, (int) expval64, exp->dscale, result);
            return;
        }
    }

    // Handle 0^non-integer (0^0 is handled by power_var_int)
    if (cmp_var(base, &const_zero) == 0) {
        set_var_from_var(&const_zero, result);
        result->dscale = NUMERIC_MIN_SIG_DIGITS;
        return;
    }

    init_var(&abs_base);
    init_var(&ln_base);
    init_var(&ln_num);

    int res_sign = NUMERIC_POS;

    // Handle negative base: requires integer exponent
    if (base->sign == NUMERIC_NEG) {
        // Verify exp is an integer
        if (exp->ndigits > 0 && exp->ndigits > exp->weight + 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                           errmsg("a negative number raised to a non-integer power yields a complex result")));

        // Determine result sign: negative if exp is odd
        if (exp->ndigits > 0 && exp->ndigits == exp->weight + 1 &&
            (exp->digits[exp->ndigits - 1] & 1))
            res_sign = NUMERIC_NEG;

        // Work with absolute value of base
        set_var_from_var(base, &abs_base);
        abs_base.sign = NUMERIC_POS;
        base = &abs_base;
    }

    // Estimate result weight for precision planning and overflow detection
    int ln_dweight = estimate_ln_dweight(base);

    // Compute low-precision ln(base) for overflow check
    int local_rscale = 8 - ln_dweight;
    local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &ln_base, local_rscale);
    mul_var(&ln_base, exp, &ln_num, local_rscale);

    // Quick overflow/underflow test using double approximation
    double val = numericvar_to_double_no_overflow(&ln_num);
    if (fabs(val) > NUMERIC_MAX_RESULT_SCALE * 3.01) {
        if (val > 0)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("value overflows numeric format")));
        zero_var(result);
        result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return;
    }

    // Calculate appropriate result scale
    val *= 0.434294481903252; // Convert to approximate decimal weight
    int rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
    rscale = Max(rscale, base->dscale);
    rscale = Max(rscale, exp->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    // Calculate precision needed for accurate computation
    int sig_digits = rscale + (int) val;
    sig_digits = Max(sig_digits, 0);

    local_rscale = sig_digits - ln_dweight + 8;
    local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    // Compute result = e^(exp * ln(base)) using full precision
    ln_var(base, &ln_base, local_rscale);
    mul_var(&ln_base, exp, &ln_num, local_rscale);
    exp_var(&ln_num, result, rscale);

    // Apply sign for negative base with odd exponent
    if (res_sign == NUMERIC_NEG && result->ndigits > 0)
        result->sign = NUMERIC_NEG;

    free_var(&ln_num);
    free_var(&ln_base);
    free_var(&abs_base);
}
```