# log_var

## Location
[src/backend/utils/adt/numeric.c:10887-10946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10887-L10946)

## Overview
Computes the logarithm of a number in a given base by calculating the ratio of natural logarithms (ln(num)/ln(base)) with intelligent precision management.

## Definition
```c
static void log_var(const NumericVar *base, const NumericVar *num, NumericVar *result)
```

## Detailed Description
This function implements logarithm computation in an arbitrary base using the mathematical identity log_base(num) = ln(num)/ln(base). The implementation includes sophisticated precision management:

1. **Precision estimation**: Uses estimate_ln_dweight() to estimate the decimal weights of ln(base) and ln(num)
2. **Scale calculation**: Determines appropriate scales for intermediate computations to ensure sufficient precision
3. **Natural logarithm computation**: Computes ln(base) and ln(num) using ln_var() with calculated scales
4. **Final division**: Divides ln(num) by ln(base) to get the result

The algorithm automatically chooses the result's display scale (dscale) to ensure at least NUMERIC_MIN_SIG_DIGITS significant digits while respecting input display scales and system limits.

## Parameters / Member Variables
- `base`: NumericVar representing the logarithm base
- `num`: NumericVar representing the number for which to compute the logarithm
- `result`: Output NumericVar to store the computed logarithm

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_ln_dweight](../e/estimate_ln_dweight.md) (for precision estimation)
  - [ln_var](ln_var.md) (for natural logarithm computation)
  - init_var, free_var (variable lifecycle management)
  - [div_var_fast](../d/div_var_fast.md) (for final division)
  - Max, Min (utility macros)
- Constants used:
  - NUMERIC_MIN_SIG_DIGITS (minimum significant digits)
  - NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MAX_DISPLAY_SCALE (display scale limits)
- Called from:
  - [numeric_log](../n/numeric_log.md) (main logarithm SQL function)

## Notes and Other Information
- This is a static function within the numeric.c module
- The function automatically determines the appropriate result scale rather than taking it as a parameter
- Precision management ensures that intermediate calculations have sufficient accuracy for the final result
- The +8 buffer in scale calculations provides extra precision margin for intermediate computations
- Located at src/backend/utils/adt/numeric.c:10887-10946

## Simplified Source

```c
static void
log_var(const NumericVar *base, const NumericVar *num, NumericVar *result)
{
    NumericVar ln_base, ln_num;

    init_var(&ln_base);
    init_var(&ln_num);

    // Estimate decimal weights for precision planning
    // This helps determine appropriate scales for intermediate calculations
    int ln_base_dweight = estimate_ln_dweight(base);
    int ln_num_dweight = estimate_ln_dweight(num);
    int result_dweight = ln_num_dweight - ln_base_dweight;

    // Calculate appropriate result scale to ensure sufficient precision
    // Aim for at least NUMERIC_MIN_SIG_DIGITS significant digits
    int rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = Max(rscale, base->dscale);           // Respect input scales
    rscale = Max(rscale, num->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    // Set scales for intermediate ln calculations with extra precision
    // Add buffer (+8) to ensure intermediate calculations don't lose accuracy
    int ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
    ln_base_rscale = Max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    int ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
    ln_num_rscale = Max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    // Compute natural logarithms with calculated precision
    ln_var(base, &ln_base, ln_base_rscale);
    ln_var(num, &ln_num, ln_num_rscale);

    // Apply logarithm change of base formula: log_base(num) = ln(num) / ln(base)
    div_var_fast(&ln_num, &ln_base, result, rscale, true);

    free_var(&ln_num);
    free_var(&ln_base);
}
```