# div_var_int64

## Location
src/backend/utils/adt/numeric.c: 9681 - 9792

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