# div_var_int64

## Location
[src/backend/utils/adt/numeric.c:9681-9792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L9681-L9792)

## Overview
Divides a PostgreSQL numeric variable by a 64-bit integer with a specified weight, implementing the quotient operation var / (ival * NBASE^ival_weight).

## Definition
```c
static void div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
                          NumericVar *result, int rscale, bool round)
```

## Detailed Description
This function performs division of a PostgreSQL numeric variable by a 64-bit integer multiplied by a power of the numeric base (NBASE). It duplicates the logic from div_var_int() but handles 64-bit divisors, implementing the same short division algorithm described in Knuth volume 2, section 4.3.1 exercise 16.

The function uses an optimized algorithm that chooses between 64-bit and 128-bit arithmetic based on the divisor size to prevent overflow. When the divisor is small enough (≤ PG_UINT64_MAX / NBASE), it uses 64-bit arithmetic for better performance. For larger divisors, it switches to 128-bit arithmetic to handle potential carry overflow.

The algorithm maintains the same structure as div_var_int() but scales up the arithmetic precision to handle the larger range of 64-bit divisors while maintaining numerical accuracy and performance.

## Parameters / Member Variables
- `var`: Input numeric variable (dividend) to be divided
- `ival`: 64-bit integer divisor value
- `ival_weight`: Weight (power of NBASE) to multiply with ival
- `result`: Output NumericVar to store the division result
- `rscale`: Number of fractional digits to retain in the result
- `round`: Boolean flag indicating whether to round (true) or truncate (false) the result

## Dependencies
- Functions called/Symbols referenced:
  - [zero_var](../z/zero_var.md) (for handling zero dividend)
  - digitbuf_alloc (memory allocation for result digits)
  - digitbuf_free (memory deallocation)
  - [round_var](../r/round_var.md) (rounding result to specified scale)
  - [trunc_var](../t/trunc_var.md) (truncating result to specified scale)
  - [strip_var](../s/strip_var.md) (removing leading/trailing zeros)
  - i64abs (64-bit absolute value function)
  - NumericDigit, NUMERIC_POS, NUMERIC_NEG, DEC_DIGITS, NBASE, PG_UINT64_MAX (numeric system constants)

- Called from (representative examples):
  - [div_var](div_var.md) (general numeric division)
  - [div_var_fast](div_var_fast.md) (optimized division path)

## Notes and Other Information
- Guards against division by zero with appropriate error reporting
- Automatically handles sign determination based on dividend and divisor signs
- Uses efficient short division algorithm with carry propagation, scaled for 64-bit divisors
- Switches between 64-bit and 128-bit arithmetic based on divisor size to optimize performance while preventing overflow
- Supports both rounding and truncation modes for result precision control
- Properly manages memory allocation and deallocation for result storage
- Handles edge cases like zero dividend and ensures at least one result digit
- Maintains code synchronization with div_var_int() - changes to one should be reflected in the other

## Simplified Source

```c
static void div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
                          NumericVar *result, int rscale, bool round) {
    // Guard against division by zero
    if (ival == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));
    }

    // Handle zero dividend
    if (var->ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    // Determine result properties
    int res_sign = (var->sign == NUMERIC_POS) == (ival > 0) ? NUMERIC_POS : NUMERIC_NEG;
    int res_weight = var->weight - ival_weight;
    int res_ndigits = Max(res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS, 1);
    if (round) res_ndigits++;  // Extra digit for rounding

    // Allocate result buffer
    NumericDigit *res_buf = digitbuf_alloc(res_ndigits + 1);
    res_buf[0] = 0;  // Spare digit for rounding
    NumericDigit *res_digits = res_buf + 1;

    // Perform short division using appropriate arithmetic precision
    uint64 divisor = i64abs(ival);

    if (divisor <= PG_UINT64_MAX / NBASE) {
        // Use 64-bit arithmetic - carry won't overflow
        uint64 carry = 0;
        for (int i = 0; i < res_ndigits; i++) {
            carry = carry * NBASE + (i < var->ndigits ? var->digits[i] : 0);
            res_digits[i] = carry / divisor;
            carry = carry % divisor;
        }
    } else {
        // Use 128-bit arithmetic - carry may exceed 64 bits
        uint128 carry = 0;
        for (int i = 0; i < res_ndigits; i++) {
            carry = carry * NBASE + (i < var->ndigits ? var->digits[i] : 0);
            res_digits[i] = carry / divisor;
            carry = carry % divisor;
        }
    }

    // Store result
    digitbuf_free(result->buf);
    result->ndigits = res_ndigits;
    result->buf = res_buf;
    result->digits = res_digits;
    result->weight = res_weight;
    result->sign = res_sign;

    // Apply rounding or truncation and cleanup
    if (round)
        round_var(result, rscale);
    else
        trunc_var(result, rscale);

    strip_var(result);  // Remove leading/trailing zeros
}
```