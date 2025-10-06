# estimate_ln_dweight

## Location
[src/backend/utils/adt/numeric.c:10687-10768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10687-L10768)

## Overview
Estimates the dweight (decimal weight) of the most significant decimal digit of the natural logarithm of a number, effectively approximating log10(abs(ln(var))) to determine appropriate rscale when computing natural logarithms.

## Definition
```c
static int estimate_ln_dweight(const NumericVar *var)
```

## Detailed Description
This function provides an estimation of the decimal weight of the natural logarithm's most significant digit without actually computing the logarithm. It uses different strategies based on the input value:

- For values close to 1 (0.9 ≤ var ≤ 1.1): Uses the approximation ln(1+x) ≈ x to handle the case where ln(var) has a negative weight
- For other values: Estimates using the first couple of digits from the input number using the formula ln(var) ≈ ln(digits) + dweight * ln(10)

The function is designed to be robust against invalid inputs (negative numbers, zero) and returns 0 for such cases rather than throwing errors, since many callers use this for preliminary range checking.

## Parameters / Member Variables
- `var`: Input NumericVar for which to estimate the natural logarithm's decimal weight

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_var](../c/cmp_var.md) (for comparing with constants)
  - init_var (for initializing temporary variables)
  - [sub_var](../s/sub_var.md) (for subtraction operations)
  - [free_var](../f/free_var.md) (for memory cleanup)
  - log10, log, fabs (standard math functions)
- Constants used:
  - NUMERIC_POS (positive number indicator)
  - const_zero_point_nine, const_one_point_one, const_one (numeric constants)
  - DEC_DIGITS, NBASE (numeric system constants)
- Called from:
  - [numeric_ln](../n/numeric_ln.md) (natural logarithm function)
  - [log_var](../l/log_var.md) (logarithm computation)
  - [power_var](../p/power_var.md) (power computation)

## Notes and Other Information
- This is a static function within the numeric.c module
- The estimation is crucial for determining precision requirements in logarithmic calculations
- The function handles edge cases gracefully by returning 0 for invalid inputs
- The magic number 2.302585092994046 represents ln(10) used in the logarithm estimation formula
- Located at src/backend/utils/adt/numeric.c:10687-10768

## Simplified Source

```c
static int
estimate_ln_dweight(const NumericVar *var)
{
    int ln_dweight;

    // Return 0 for invalid inputs (let caller handle errors)
    if (var->sign != NUMERIC_POS)
        return 0;

    // Special handling for values close to 1 (0.9 <= var <= 1.1)
    if (cmp_var(var, &const_zero_point_nine) >= 0 &&
        cmp_var(var, &const_one_point_one) <= 0) {

        // For ln(1+x) where x is small, ln(1+x) ≈ x
        NumericVar x;
        init_var(&x);
        sub_var(var, &const_one, &x);  // x = var - 1

        if (x.ndigits > 0) {
            // Use weight of most significant decimal digit of x
            ln_dweight = x.weight * DEC_DIGITS + (int) log10(x.digits[0]);
        } else {
            // x = 0, so ln(1) = 0 exactly
            ln_dweight = 0;
        }

        free_var(&x);
    } else {
        // For values not close to 1, estimate using first few digits
        if (var->ndigits > 0) {
            int digits = var->digits[0];
            int dweight = var->weight * DEC_DIGITS;

            // Include second digit for better accuracy
            if (var->ndigits > 1) {
                digits = digits * NBASE + var->digits[1];
                dweight -= DEC_DIGITS;
            }

            // Apply formula: ln(var) ≈ ln(digits) + dweight * ln(10)
            // where var ≈ digits * 10^dweight
            double ln_var = log((double) digits) + dweight * 2.302585092994046;
            ln_dweight = (int) log10(fabs(ln_var));
        } else {
            // Return 0 for ln(0) (let caller handle the error)
            ln_dweight = 0;
        }
    }

    return ln_dweight;
}
```