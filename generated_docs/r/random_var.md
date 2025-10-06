# random_var

## Location
[src/backend/utils/adt/numeric.c:11339-11521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11339-L11521)

## Overview
Generates a uniformly distributed random numeric value within a specified range [rmin, rmax] using PostgreSQL's internal NumericVar representation.

## Definition

```c
static void
random_var(pg_prng_state *state, const NumericVar *rmin,
		   const NumericVar *rmax, NumericVar *result)
```
## Detailed Description
This function generates a random numeric value uniformly distributed within the closed interval [rmin, rmax]. The implementation uses sophisticated algorithms to ensure true uniform distribution while handling PostgreSQL's variable-precision decimal arithmetic. 

The function works by:
1. Computing the range length (rmax - rmin) and validating bounds
2. Handling the special case of empty ranges (rmin == rmax)
3. For non-empty ranges, generating a random value in [0, range_length] and then shifting it to the target range

The core challenge is generating uniform random values for arbitrary-precision decimals. The algorithm addresses this by:
- Using the first 4 NBASE digits to form a 64-bit integer for efficient random generation
- Setting remaining digits to '9' to create a slightly larger range
- Using rejection sampling to ensure true uniform distribution
- Handling decimal scale requirements by ensuring the final digit is a multiple of the appropriate power of 10

## Parameters / Member Variables
- `*state`: Pointer to PostgreSQL's pseudo-random number generator state
- `*rmin`: Pointer to NumericVar representing the minimum bound of the range (inclusive)
- `*rmax`: Pointer to NumericVar representing the maximum bound of the range (inclusive)
- `*result`: Pointer to NumericVar where the generated random value will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [sub_var](../s/sub_var.md) (computes range length)
  - init_var, alloc_var, free_var (memory management)
  - [set_var_from_var](../s/set_var_from_var.md) (copying values)
  - [pg_prng_uint64_range](../p/pg_prng_uint64_range.md) (random number generation)
  - [cmp_var](../c/cmp_var.md) (comparison for rejection sampling)
  - [add_var](../a/add_var.md) (shifting result to target range)
  - [strip_var](../s/strip_var.md) (removing leading/trailing zeros)
  - Constants: NUMERIC_NEG, NUMERIC_POS, NBASE, DEC_DIGITS
- Called from (representative examples):
  - [random_numeric](random_numeric.md) (public numeric random function)
  - NUMERIC_CAN_BE_SHORT (numeric optimization checks)

## Notes and Other Information
- Static function internal to numeric.c, not part of the public API
- Implements rejection sampling to ensure uniform distribution with probability of rejection less than 1e-13
- Handles arbitrary precision decimals while maintaining efficiency through 64-bit arithmetic where possible
- Validates that rmin <= rmax, throwing an error for invalid ranges
- Properly handles decimal scale by ensuring the final digit aligns with the required precision
- The algorithm is optimized for performance by processing digits in groups of 4 when possible
- Memory allocation and cleanup is handled internally with proper error handling

## Simplified Source

```c
static void random_var(pg_prng_state *state, const NumericVar *rmin,
                      const NumericVar *rmax, NumericVar *result) {
    int rscale = Max(rmin->dscale, rmax->dscale);
    NumericVar rlen;

    // Compute range length = rmax - rmin
    init_var(&rlen);
    sub_var(rmax, rmin, &rlen);

    // Check bounds
    if (rlen.sign == NUMERIC_NEG)
        ereport(ERROR, (errmsg("lower bound must be <= upper bound")));

    // Handle empty range case
    if (rlen.ndigits == 0) {
        set_var_from_var(rmin, result);
        result->dscale = rscale;
        free_var(&rlen);
        return;
    }

    // Calculate result structure
    int res_ndigits = rlen.weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    int n = ((rscale + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale;
    int pow10 = 1;
    for (int i = 0; i < n; i++)
        pow10 *= 10;

    // Create 64-bit representation from first 4 digits
    uint64 rlen64 = rlen.digits[0];
    int rlen64_ndigits = 1;
    while (rlen64_ndigits < res_ndigits && rlen64_ndigits < 4) {
        rlen64 *= NBASE;
        if (rlen64_ndigits < rlen.ndigits)
            rlen64 += rlen.digits[rlen64_ndigits];
        rlen64_ndigits++;
    }

    // Rejection sampling loop for uniform distribution
    do {
        alloc_var(result, res_ndigits);
        result->sign = NUMERIC_POS;
        result->weight = rlen.weight;
        result->dscale = rscale;

        // Generate random digits
        uint64 rand;
        if (rlen64_ndigits == res_ndigits && pow10 != 1)
            rand = pg_prng_uint64_range(state, 0, rlen64 / pow10) * pow10;
        else
            rand = pg_prng_uint64_range(state, 0, rlen64);

        // Fill in the digits
        for (int i = rlen64_ndigits - 1; i >= 0; i--) {
            result->digits[i] = rand % NBASE;
            rand = rand / NBASE;
        }

        // Fill remaining digits with random values
        for (int i = rlen64_ndigits; i < res_ndigits; i++) {
            if (i == res_ndigits - 1 && pow10 != 1)
                result->digits[i] = pg_prng_uint64_range(state, 0, NBASE / pow10 - 1) * pow10;
            else
                result->digits[i] = pg_prng_uint64_range(state, 0, NBASE - 1);
        }

        strip_var(result);

    } while (cmp_var(result, &rlen) > 0);  // Reject if > range

    // Shift result to target range
    add_var(result, rmin, result);
    free_var(&rlen);
}
```