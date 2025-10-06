# exp_var

## Location
[src/backend/utils/adt/numeric.c:10558-10686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10558-L10686)

## Overview
The `exp_var` function computes the exponential function (e^x) for a numeric value using Taylor series expansion with range reduction techniques, providing high-precision exponential calculation for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void exp_var(const NumericVar *arg, NumericVar *result, int rscale)
```

## Detailed Description
This function implements the mathematical exponential function e^x using a sophisticated algorithm that combines Taylor series expansion with range reduction for optimal precision and performance. The implementation includes several key optimizations:

1. **Overflow protection**: Guards against overflow and underflow by checking argument bounds
2. **Range reduction**: Reduces the argument to the range [-0.01, 0.01] by repeated division by 2^ndiv2
3. **Taylor series**: Uses the series exp(x) = 1 + x + x²/2! + x³/3! + ... for computation
4. **Precision management**: Dynamically adjusts working precision based on expected result magnitude
5. **Compensation**: Reverses the range reduction by squaring the result ndiv2 times

The algorithm works by:
1. Converting input to double to estimate result magnitude and detect overflow
2. Reducing argument range through division by powers of 2
3. Computing Taylor series expansion with appropriate precision
4. Compensating for range reduction through repeated squaring
5. Rounding to the requested precision

## Parameters / Member Variables
- `arg`: Input `NumericVar` containing the exponent value (x in e^x)
- `result`: Output `NumericVar` where the exponential result will be stored
- `rscale`: Number of fractional digits in the result

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize `NumericVar` structures
  - [set_var_from_var](../s/set_var_from_var.md): Copy one `NumericVar` to another
  - [numericvar_to_double_no_overflow](../n/numericvar_to_double_no_overflow.md): Convert `NumericVar` to double for estimation
  - [zero_var](../z/zero_var.md): Set a `NumericVar` to zero
  - [div_var_int](../d/div_var_int.md): Divide `NumericVar` by integer
  - [add_var](../a/add_var.md): Add two `NumericVar` values
  - [mul_var](../m/mul_var.md): Multiply two `NumericVar` values
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [free_var](../f/free_var.md): Free memory associated with `NumericVar`
  - Constants: `const_one`, `NUMERIC_MAX_RESULT_SCALE`, `NUMERIC_MIN_DISPLAY_SCALE`, `DEC_DIGITS`

- Called from (representative examples):
  - [numeric_exp](../n/numeric_exp.md): SQL-callable exponential function
  - [power_var](../p/power_var.md): Used in power function computations (for non-integer exponents)

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- Uses Taylor series expansion which converges rapidly for small arguments (hence the range reduction)
- Includes comprehensive overflow detection following PostgreSQL's numeric limits
- The range reduction technique (dividing by 2^n then squaring n times) maintains precision while improving convergence
- Performance is optimized by adjusting working precision dynamically as computation proceeds
- Handles edge cases like very large positive arguments (overflow) and very large negative arguments (underflow to zero)
- The algorithm is numerically stable and provides results accurate to the specified precision
- Used as a building block for other transcendental functions in PostgreSQL's numeric system
- The convergence criterion stops when Taylor series terms become negligible relative to the working precision

## Simplified Source

```c
static void
exp_var(const NumericVar *arg, NumericVar *result, int rscale)
{
    NumericVar x, elem;

    init_var(&x);
    init_var(&elem);

    set_var_from_var(arg, &x);

    // Estimate result magnitude to detect overflow
    double val = numericvar_to_double_no_overflow(&x);

    // Guard against overflow/underflow (limit from power_var too)
    if (fabs(val) >= NUMERIC_MAX_RESULT_SCALE * 3) {
        if (val > 0)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("value overflows numeric format")));
        // Large negative exponent -> result approaches 0
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Calculate expected decimal weight: log10(e^x) = x * log10(e)
    int dweight = (int) (val * 0.434294481903252);

    // Range reduction: reduce x to [-0.01, 0.01] for better convergence
    int ndiv2 = 0;
    if (fabs(val) > 0.01) {
        ndiv2 = 1;
        val /= 2;

        while (fabs(val) > 0.01) {
            ndiv2++;
            val /= 2;
        }

        // Divide x by 2^ndiv2
        int local_rscale = x.dscale + ndiv2;
        div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, true);
    }

    // Set working precision for Taylor series
    // Need extra precision for final squaring operations
    int sig_digits = 1 + dweight + rscale + (int) (ndiv2 * 0.301029995663981);
    sig_digits = Max(sig_digits, 0) + 8;
    int local_rscale = sig_digits - 1;

    // Compute Taylor series: exp(x) = 1 + x + x^2/2! + x^3/3! + ...
    add_var(&const_one, &x, result);  // Start with 1 + x

    // Compute second term: x^2/2!
    mul_var(&x, &x, &elem, local_rscale);
    int ni = 2;
    div_var_int(&elem, ni, 0, &elem, local_rscale, true);

    // Continue series until terms become negligible
    while (elem.ndigits != 0) {
        add_var(result, &elem, result);

        // Next term: multiply by x and divide by next factorial
        mul_var(&elem, &x, &elem, local_rscale);
        ni++;
        div_var_int(&elem, ni, 0, &elem, local_rscale, true);
    }

    // Compensate for range reduction: square result ndiv2 times
    // (since exp(x) = exp(x/2^n)^(2^n))
    while (ndiv2-- > 0) {
        // Reduce precision as result grows
        local_rscale = sig_digits - result->weight * 2 * DEC_DIGITS;
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        mul_var(result, result, result, local_rscale);
    }

    // Round to requested precision
    round_var(result, rscale);

    free_var(&x);
    free_var(&elem);
}
```