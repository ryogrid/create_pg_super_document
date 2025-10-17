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

## Simplified Source

```c
static void
power_var_int(const NumericVar *base, int exp, int exp_dscale, NumericVar *result)
{
    double f;
    int rscale;
    int sig_digits;
    unsigned int mask;
    bool neg;
    NumericVar base_prod;
    int local_rscale;

    // Estimate result weight using double precision to prevent overflow
    if (base->ndigits != 0) {
        f = base->digits[0];
        int p = base->weight * DEC_DIGITS;

        // Build approximate base value
        for (int i = 1; i < base->ndigits && i * DEC_DIGITS < 16; i++) {
            f = f * NBASE + base->digits[i];
            p -= DEC_DIGITS;
        }

        f = exp * (log10(f) + p);  // approximate result weight
    } else {
        f = 0;  // base is zero
    }

    // Check for overflow/underflow
    if (f > (NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("value overflows numeric format")));

    if (f + 1 < -NUMERIC_MAX_DISPLAY_SCALE) {
        zero_var(result);
        result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return;
    }

    // Choose result scale with sufficient significant digits
    rscale = NUMERIC_MIN_SIG_DIGITS - (int) f;
    rscale = Max(rscale, base->dscale);
    rscale = Max(rscale, exp_dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    // Handle special cases
    switch (exp) {
        case 0:
            set_var_from_var(&const_one, result);  // Any number^0 = 1
            result->dscale = rscale;
            return;
        case 1:
            set_var_from_var(base, result);        // base^1 = base
            round_var(result, rscale);
            return;
        case -1:
            div_var(&const_one, base, result, rscale, true);  // base^-1 = 1/base
            return;
        case 2:
            mul_var(base, base, result, rscale);   // base^2 = base * base
            return;
    }

    // Handle zero base
    if (base->ndigits == 0) {
        if (exp < 0)
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                           errmsg("division by zero")));
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Binary exponentiation algorithm
    sig_digits = 1 + rscale + (int) f;
    sig_digits += (int) log(fabs((double) exp)) + 8;  // error margin

    neg = (exp < 0);
    mask = abs(exp);

    init_var(&base_prod);
    set_var_from_var(base, &base_prod);

    // Initialize result based on least significant bit
    if (mask & 1)
        set_var_from_var(base, result);
    else
        set_var_from_var(&const_one, result);

    // Process remaining bits by repeated squaring
    while ((mask >>= 1) > 0) {
        // Square base_prod
        local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
        local_rscale = Min(local_rscale, 2 * base_prod.dscale);
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

        mul_var(&base_prod, &base_prod, &base_prod, local_rscale);

        // If bit is set, multiply into result
        if (mask & 1) {
            local_rscale = sig_digits -
                          (base_prod.weight + result->weight) * DEC_DIGITS;
            local_rscale = Min(local_rscale,
                              base_prod.dscale + result->dscale);
            local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

            mul_var(&base_prod, result, result, local_rscale);
        }

        // Check for overflow during computation
        if (base_prod.weight > NUMERIC_WEIGHT_MAX ||
            result->weight > NUMERIC_WEIGHT_MAX) {
            if (!neg)
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                               errmsg("value overflows numeric format")));
            zero_var(result);
            neg = false;
            break;
        }
    }

    free_var(&base_prod);

    // Handle negative exponent by taking reciprocal
    if (neg)
        div_var_fast(&const_one, result, result, rscale, true);
    else
        round_var(result, rscale);
}
```