# div_var

## Location
[src/backend/utils/adt/numeric.c:8893-9200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8893-L9200)

## Overview
Performs division of two NumericVar values using long division algorithm with optimizations for short divisors and precise control over result precision and rounding.

## Definition

```c
static void
div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale, bool round)
```
## Detailed Description
The  function implements division at the variable level for PostgreSQL's NUMERIC data type. It uses several optimization strategies:

1. **Division by zero check**: Validates that the divisor is not zero or unnormalized
2. **Short divisor optimization**: Delegates to faster  for 1-2 digit divisors, or  for 3-4 digit divisors on 128-bit platforms
3. **Long division algorithm**: Uses Knuth's Algorithm 4.3.1D for multi-digit division
4. **Normalization**: Scales both dividend and divisor to ensure the first divisor digit is >= NBASE/2 for algorithm stability
5. **Precision control**: Calculates exactly  fractional digits with optional rounding or truncation

The algorithm estimates quotient digits using the first two dividend digits, adjusts for accuracy, then performs subtract-and-shift operations similar to manual long division.

## Parameters / Member Variables
- : Dividend NumericVar (input)
- : Divisor NumericVar (input)
- : NumericVar to store the division result (output)
- : Target fractional digits in the result
- : If true, round at rscale digits; if false, truncate

## Dependencies
- Functions called/Symbols referenced:
  - ereport/ERROR (for division by zero)
  - [div_var_int](div_var_int.md) (for short integer divisors)
  - [div_var_int64](div_var_int64.md) (for 64-bit integer divisors on 128-bit platforms)
  - [zero_var](../z/zero_var.md) (for zero dividend case)
  - [alloc_var](../a/alloc_var.md) (for result allocation)
  - [round_var](../r/round_var.md) (for rounding result)
  - [trunc_var](../t/trunc_var.md) (for truncating result)
  - [strip_var](../s/strip_var.md) (for removing leading/trailing zeros)
  - [palloc0](../p/palloc0.md)/pfree (for memory management)
  - memcpy (for copying digit arrays)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE/HALF_NBASE (numeric base constants)
  - DEC_DIGITS (digits per NumericDigit)
- Called from (representative examples):
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md)
  - [numeric_div_trunc](../n/numeric_div_trunc.md)
  - [numeric_lcm](../n/numeric_lcm.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)
  - [mod_var](../m/mod_var.md)
  - [power_var_int](../p/power_var_int.md)
  - [get_str_from_var_sci](../g/get_str_from_var_sci.md)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Implements Knuth's Algorithm 4.3.1D for long division
- Automatically delegates to optimized short division routines when possible
- The algorithm ensures the first divisor digit is >= NBASE/2 for numerical stability
- Supports both rounding and truncation modes for result precision
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Uses temporary memory allocation that is cleaned up automatically
- The quotient estimation and correction steps ensure mathematical accuracy

## Simplified Source

```c
static void div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
                   int rscale, bool round)
{
    int div_ndigits, res_ndigits, res_sign, res_weight;
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit *dividend, *divisor, *res_digits;

    // Check for division by zero
    if (var2ndigits == 0 || var2->digits[0] == 0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));

    // Optimize for short divisors (1-2 digits)
    if (var2ndigits <= 2) {
        int idivisor = var2->digits[0];
        int idivisor_weight = var2->weight;
        if (var2ndigits == 2) {
            idivisor = idivisor * NBASE + var2->digits[1];
            idivisor_weight--;
        }
        if (var2->sign == NUMERIC_NEG)
            idivisor = -idivisor;

        div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }

#ifdef HAVE_INT128
    // Optimize for medium divisors (3-4 digits) on 128-bit platforms
    if (var2ndigits <= 4) {
        int64 idivisor = var2->digits[0];
        int idivisor_weight = var2->weight;
        for (int i = 1; i < var2ndigits; i++) {
            idivisor = idivisor * NBASE + var2->digits[i];
            idivisor_weight--;
        }
        if (var2->sign == NUMERIC_NEG)
            idivisor = -idivisor;

        div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }
#endif

    // Handle zero dividend
    if (var1ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Calculate result properties
    res_sign = (var1->sign == var2->sign) ? NUMERIC_POS : NUMERIC_NEG;
    res_weight = var1->weight - var2->weight;
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = Max(res_ndigits, 1);
    if (round)
        res_ndigits++;

    // Setup working memory for long division
    div_ndigits = Max(res_ndigits + var2ndigits, var1ndigits);
    dividend = (NumericDigit *) palloc0((div_ndigits + var2ndigits + 2) * sizeof(NumericDigit));
    divisor = dividend + (div_ndigits + 1);

    // Copy input data to working arrays
    memcpy(dividend + 1, var1->digits, var1ndigits * sizeof(NumericDigit));
    memcpy(divisor + 1, var2->digits, var2ndigits * sizeof(NumericDigit));

    // Allocate result storage
    alloc_var(result, res_ndigits);
    res_digits = result->digits;

    // Normalize divisor for algorithm stability (ensure first digit >= NBASE/2)
    if (divisor[1] < HALF_NBASE) {
        int d = NBASE / (divisor[1] + 1);
        // Scale both divisor and dividend by factor d
        // [Detailed scaling code omitted for brevity]
    }

    // Main long division loop using Knuth's Algorithm 4.3.1D
    for (int j = 0; j < res_ndigits; j++) {
        // Estimate quotient digit from first two dividend digits
        int next2digits = dividend[j] * NBASE + dividend[j + 1];
        int qhat;

        if (next2digits == 0) {
            res_digits[j] = 0;
            continue;
        }

        // Calculate initial quotient estimate
        if (dividend[j] == divisor[1])
            qhat = NBASE - 1;
        else
            qhat = next2digits / divisor[1];

        // Refine quotient estimate for accuracy
        while (divisor[2] * qhat >
               (next2digits - qhat * divisor[1]) * NBASE + dividend[j + 2])
            qhat--;

        // Subtract qhat * divisor from working dividend
        if (qhat > 0) {
            // [Detailed subtraction and correction code omitted]
            // Includes borrow handling and quotient adjustment if needed
        }

        res_digits[j] = qhat;
    }

    pfree(dividend);

    // Finalize result
    result->weight = res_weight;
    result->sign = res_sign;

    // Apply rounding or truncation to target precision
    if (round)
        round_var(result, rscale);
    else
        trunc_var(result, rscale);

    strip_var(result);  // Remove leading/trailing zeros
}
```