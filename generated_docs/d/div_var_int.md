# div_var_int

## Location
[src/backend/utils/adt/numeric.c:9565-9680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L9565-L9680)

## Overview
Divides a PostgreSQL numeric variable by a 32-bit integer with a specified weight, implementing the quotient operation var / (ival * NBASE^ival_weight).

## Definition
```c
static void div_var_int(const NumericVar *var, int ival, int ival_weight,
                        NumericVar *result, int rscale, bool round)
```

## Detailed Description
This function performs division of a PostgreSQL numeric variable by a 32-bit integer multiplied by a power of the numeric base (NBASE). It implements the short division algorithm described in Knuth volume 2, section 4.3.1 exercise 16, with adaptations to handle divisors that may exceed the internal base.

The function uses an optimized algorithm that chooses between 32-bit and 64-bit arithmetic based on the divisor size to prevent overflow. When the divisor is small enough (≤ UINT_MAX / NBASE), it uses 32-bit arithmetic for better performance. For larger divisors, it switches to 64-bit arithmetic to handle potential carry overflow.

The division result is stored in the provided result NumericVar, with proper handling of sign determination, weight calculation, and digit precision management.

## Parameters / Member Variables
- `var`: Input numeric variable (dividend) to be divided
- `ival`: 32-bit integer divisor value
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
  - NumericDigit, NUMERIC_POS, NUMERIC_NEG, DEC_DIGITS, NBASE (numeric system constants)

- Called from (representative examples):
  - [div_var](div_var.md) (general numeric division)
  - [div_var_fast](div_var_fast.md) (optimized division path)
  - [exp_var](../e/exp_var.md) (exponential function implementation)
  - [ln_var](../l/ln_var.md) (natural logarithm implementation)

## Notes and Other Information
- Guards against division by zero with appropriate error reporting
- Automatically handles sign determination based on dividend and divisor signs
- Uses efficient short division algorithm with carry propagation
- Switches between 32-bit and 64-bit arithmetic based on divisor size to optimize performance while preventing overflow
- Supports both rounding and truncation modes for result precision control
- Properly manages memory allocation and deallocation for result storage
- Handles edge cases like zero dividend and ensures at least one result digit