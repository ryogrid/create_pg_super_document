# sqrt_var

## Location
[src/backend/utils/adt/numeric.c:10078-10557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L10078-L10557)

## Overview
The `sqrt_var` function computes the square root of a numeric value using the Karatsuba Square Root algorithm, providing high-precision square root calculation for PostgreSQL's `NumericVar` data type.

## Definition
```c
static void sqrt_var(const NumericVar *arg, NumericVar *result, int rscale)
```

## Detailed Description
This function implements a sophisticated square root algorithm based on the Karatsuba Square Root method, which efficiently computes square roots of arbitrary-precision numbers. The algorithm is implemented iteratively rather than recursively for better performance and memory management.

Key features:
1. **Input validation**: Handles zero input and rejects negative numbers with appropriate SQL error codes
2. **Multi-stage computation**: Uses different precision levels (int64, int128 if available, then NumericVar) for optimal performance
3. **Karatsuba algorithm**: Recursively breaks down the problem into smaller square root computations
4. **Newton's method**: Uses Newton-Raphson iteration for initial approximation refinement
5. **Precision control**: Allows negative rscale for rounding before the decimal point

The algorithm treats inputs as integers during computation and works by repeatedly applying the recursive Karatsuba formula:
- SqrtRem(n = a3*b³ + a2*b² + a1*b + a0)
- Computes square root and remainder through divide-and-conquer

## Parameters / Member Variables
- `arg`: Input `NumericVar` containing the numeric value to compute square root of
- `result`: Output `NumericVar` where the square root result will be stored
- `rscale`: Number of decimal places in the result (can be negative for rounding before decimal point)

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_var](../c/cmp_var.md): Compare two `NumericVar` values
  - [zero_var](../z/zero_var.md): Set a `NumericVar` to zero
  - `init_var`: Initialize `NumericVar` structures
  - [set_var_from_var](set_var_from_var.md): Copy one `NumericVar` to another
  - [add_var](../a/add_var.md), `sub_var`, `mul_var`: Arithmetic operations
  - [div_mod_var](../d/div_mod_var.md): Division with remainder
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [strip_var](strip_var.md): Remove leading/trailing zeros
  - [alloc_var](../a/alloc_var.md): Allocate memory for `NumericVar`
  - [free_var](../f/free_var.md): Free memory associated with `NumericVar`
  - [int64_to_numericvar](../i/int64_to_numericvar.md), `int128_to_numericvar`: Convert integers to `NumericVar`
  - Various constants: `const_zero`, `const_one`, `NUMERIC_POS`, `NUMERIC_NEG`

- Called from (representative examples):
  - [numeric_sqrt](../n/numeric_sqrt.md): SQL-callable square root function
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md): Used in standard deviation calculations
  - [ln_var](../l/ln_var.md): Used in natural logarithm computations

## Notes and Other Information
- This is a static function internal to the numeric data type implementation
- Implements the advanced Karatsuba Square Root algorithm for optimal performance on large numbers
- Uses multiple precision levels: starts with double precision for small inputs, then int64, int128 (if available), and finally full NumericVar arithmetic
- Includes comprehensive input validation following SQL2003 standards for power functions
- The algorithm guarantees convergence and handles edge cases like perfect squares
- Performance is optimized by minimizing expensive numeric operations through staged computation
- Supports very high precision calculations limited only by available memory
- The implementation is interrupt-safe for long-running computations

## Simplified Source

```c
static void
sqrt_var(const NumericVar *arg, NumericVar *result, int rscale)
{
    // Handle zero input
    if (cmp_var(arg, &const_zero) == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Reject negative input (SQL standard)
    if (cmp_var(arg, &const_zero) < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                       errmsg("cannot take square root of a negative number")));

    // Initialize working variables
    NumericVar s_var, r_var, a0_var, a1_var, q_var, u_var;
    init_var(&s_var); init_var(&r_var); init_var(&a0_var);
    init_var(&a1_var); init_var(&q_var); init_var(&u_var);

    // Calculate result weight and required precision
    int res_weight = (arg->weight >= 0) ? arg->weight / 2 :
                     -((-arg->weight - 1) / 2 + 1);
    int res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS;
    res_ndigits = Max(res_ndigits, 1);

    // Determine number of source digits needed
    int src_ndigits = arg->weight + 1 + (res_ndigits - res_weight - 1) * 2;
    src_ndigits = Max(src_ndigits, 1);

    // Plan iteration strategy using Karatsuba algorithm
    int step = 0;
    int ndigits[32];
    while ((ndigits[step] = src_ndigits) > 4) {
        int blen = src_ndigits / 4;
        if (blen * 4 == src_ndigits && arg->digits[0] < NBASE / 4)
            blen--;
        src_ndigits -= 2 * blen;
        step++;
    }

    // First iteration: handle small inputs with int64 arithmetic
    int64 arg_int64 = arg->digits[0];
    for (int i = 1; i < src_ndigits; i++) {
        arg_int64 *= NBASE;
        if (i < arg->ndigits)
            arg_int64 += arg->digits[i];
    }

    // Initial square root estimate using floating point
    int64 s_int64 = (int64) sqrt((double) arg_int64);
    int64 r_int64 = arg_int64 - s_int64 * s_int64;

    // Newton's method refinement: x -> (x + n/x) / 2
    while (r_int64 < 0 || r_int64 > 2 * s_int64) {
        s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
        r_int64 = arg_int64 - s_int64 * s_int64;
    }

    // Process medium-sized numbers with int64
    int src_idx = src_ndigits;
    step--;
    while (step >= 0 && ndigits[step] <= 8) {
        int blen = (ndigits[step] - src_idx) / 2;

        // Extract coefficients a1 and a0
        int a1 = 0, a0 = 0, b = 1;
        for (int i = 0; i < blen; i++, src_idx++) {
            b *= NBASE;
            a1 *= NBASE;
            if (src_idx < arg->ndigits)
                a1 += arg->digits[src_idx];
        }
        for (int i = 0; i < blen; i++, src_idx++)  {
            a0 *= NBASE;
            if (src_idx < arg->ndigits)
                a0 += arg->digits[src_idx];
        }

        // Apply Karatsuba step: (q,u) = DivRem(r*b + a1, 2*s)
        int64 numer = r_int64 * b + a1;
        int64 denom = 2 * s_int64;
        int64 q = numer / denom;
        int64 u = numer - q * denom;

        // Update s = s*b + q, r = u*b + a0 - q^2
        s_int64 = s_int64 * b + q;
        r_int64 = u * b + a0 - q * q;

        // Correction if remainder is negative
        if (r_int64 < 0) {
            r_int64 += s_int64;
            s_int64--;
            r_int64 += s_int64;
        }
        step--;
    }

    // Convert to NumericVar for high-precision iterations
    int64_to_numericvar(s_int64, &s_var);
    if (step >= 0)
        int64_to_numericvar(r_int64, &r_var);

    // Handle remaining iterations with full NumericVar arithmetic
    while (step >= 0) {
        src_ndigits = ndigits[step];
        int blen = (src_ndigits - src_idx) / 2;

        // Extract a1 and a0 as NumericVar
        // [NumericVar extraction logic simplified...]

        // Apply Karatsuba iteration using div_mod_var
        set_var_from_var(&r_var, &q_var);
        q_var.weight += blen;
        add_var(&q_var, &a1_var, &q_var);
        add_var(&s_var, &s_var, &u_var);
        div_mod_var(&q_var, &u_var, &q_var, &u_var);

        // Update s and compute r
        s_var.weight += blen;
        add_var(&s_var, &q_var, &s_var);

        if (step > 0) {
            u_var.weight += blen;
            add_var(&u_var, &a0_var, &u_var);
            mul_var(&q_var, &q_var, &q_var, 0);
            sub_var(&u_var, &q_var, &r_var);

            if (r_var.sign == NUMERIC_NEG) {
                add_var(&r_var, &s_var, &r_var);
                sub_var(&s_var, &const_one, &s_var);
                add_var(&r_var, &s_var, &r_var);
            }
        }
        step--;
    }

    // Finalize result
    set_var_from_var(&s_var, result);
    result->weight = res_weight;
    result->sign = NUMERIC_POS;

    round_var(result, rscale);
    strip_var(result);

    // Clean up
    free_var(&s_var); free_var(&r_var); free_var(&a0_var);
    free_var(&a1_var); free_var(&q_var); free_var(&u_var);
}
```