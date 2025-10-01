# mul_var

## Location
[src/backend/utils/adt/numeric.c:8685-8892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8685-L8892)

## Overview
Performs multiplication of two NumericVar values using schoolbook multiplication algorithm, with optimizations for performance and precision control.

## Definition

```c
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale)
```
## Detailed Description
The  function implements multiplication at the variable level for PostgreSQL's NUMERIC data type. It uses a schoolbook multiplication algorithm with several optimizations:

1. **Input reordering**: Arranges var1 to be the shorter number to minimize iterations
2. **Zero handling**: Early return for zero operands  
3. **Sign determination**: Results are positive if both operands have the same sign, negative otherwise
4. **Precision control**: Truncates computation based on rscale with guard digits for accuracy
5. **Overflow prevention**: Uses signed integers with normalization to prevent overflow
6. **Performance optimization**: The inner loop is designed to be auto-vectorizable

The algorithm accumulates partial products in an integer array, periodically normalizing to prevent overflow, then performs a final carry propagation pass before rounding to the requested scale.

## Parameters / Member Variables
- : First NumericVar operand (input)
- : Second NumericVar operand (input)  
- : NumericVar to store the multiplication result (output)
- : Target fractional digits for rounding the result

## Dependencies
- Functions called/Symbols referenced:
  - [zero_var](../z/zero_var.md) (for zero result cases)
  - [alloc_var](../a/alloc_var.md) (for result allocation)
  - [round_var](../r/round_var.md) (for final rounding)
  - [strip_var](../s/strip_var.md) (for removing leading/trailing zeros)
  - [palloc0](../p/palloc0.md) (for temporary array allocation)
  - [pfree](../p/pfree.md) (for memory cleanup)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE (numeric base constant)
  - DEC_DIGITS (digits per NumericDigit)
  - MUL_GUARD_DIGITS (guard digits for precision)
- Called from (representative examples):
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md)
  - [numeric_lcm](../n/numeric_lcm.md)
  - [numeric_fac](../n/numeric_fac.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)
  - [div_mod_var](../d/div_mod_var.md)
  - [sqrt_var](../s/sqrt_var.md)
  - [exp_var](../e/exp_var.md)
  - [ln_var](../l/ln_var.md)
  - [power_var](../p/power_var.md)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Uses an optimized schoolbook multiplication algorithm
- Implements carry propagation normalization to prevent integer overflow
- The inner multiplication loop is designed for auto-vectorization
- Guard digits (MUL_GUARD_DIGITS) help maintain precision during truncation
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Memory allocation for temporary arrays is cleaned up automatically

## Simplified Source

```c
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result, int rscale)
{
    int res_ndigits, res_sign, res_weight;
    int *dig;  // Accumulator array
    int maxdig = 0;
    int var1ndigits, var2ndigits;
    NumericDigit *var1digits, *var2digits;

    // Optimization: arrange shorter number as var1 for better performance
    if (var1->ndigits > var2->ndigits) {
        const NumericVar *tmp = var1;
        var1 = var2;
        var2 = tmp;
    }

    // Local copies for speed
    var1ndigits = var1->ndigits;
    var2ndigits = var2->ndigits;
    var1digits = var1->digits;
    var2digits = var2->digits;

    // Handle zero operands
    if (var1ndigits == 0 || var2ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Determine result sign and weight
    res_sign = (var1->sign == var2->sign) ? NUMERIC_POS : NUMERIC_NEG;
    res_weight = var1->weight + var2->weight + 2;

    // Calculate number of result digits needed
    res_ndigits = var1ndigits + var2ndigits + 1;
    int maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    res_ndigits = Min(res_ndigits, maxdigits);

    if (res_ndigits < 3) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Allocate accumulator array
    dig = (int *) palloc0(res_ndigits * sizeof(int));

    // Main multiplication loop - schoolbook algorithm
    for (int i1 = Min(var1ndigits - 1, res_ndigits - 3); i1 >= 0; i1--) {
        NumericDigit var1digit = var1digits[i1];
        if (var1digit == 0) continue;

        // Check if normalization needed to prevent overflow
        maxdig += var1digit;
        if (maxdig > (INT_MAX - INT_MAX / NBASE) / (NBASE - 1)) {
            // Normalize carries
            int carry = 0;
            for (int i = res_ndigits - 1; i >= 0; i--) {
                int newdig = dig[i] + carry;
                carry = (newdig >= NBASE) ? newdig / NBASE : 0;
                dig[i] = newdig - carry * NBASE;
            }
            maxdig = 1 + var1digit;
        }

        // Inner multiplication loop - multiply var1digit by var2
        int i2limit = Min(var2ndigits, res_ndigits - i1 - 2);
        for (int i2 = 0; i2 < i2limit; i2++) {
            dig[i1 + 2 + i2] += var1digit * var2digits[i2];
        }
    }

    // Final carry propagation and result storage
    alloc_var(result, res_ndigits);
    int carry = 0;
    for (int i = res_ndigits - 1; i >= 0; i--) {
        int newdig = dig[i] + carry;
        carry = (newdig >= NBASE) ? newdig / NBASE : 0;
        result->digits[i] = newdig - carry * NBASE;
    }

    pfree(dig);

    // Set result properties and round to target precision
    result->weight = res_weight;
    result->sign = res_sign;
    round_var(result, rscale);
    strip_var(result);
}
```