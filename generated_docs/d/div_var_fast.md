# div_var_fast

## Location
[src/backend/utils/adt/numeric.c:9201-9564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L9201-L9564)

## Overview
A fast division algorithm for NumericVar values using floating-point estimation and the FM library approach, optimized for transcendental function calculations where approximate results are acceptable.

## Definition

```c
static void
div_var_fast(const NumericVar *var1, const NumericVar *var2,
			 NumericVar *result, int rscale, bool round)
```
## Detailed Description
The  function implements a fast division algorithm for PostgreSQL's NUMERIC data type, designed as an alternative to Knuth's schoolbook division used in . Key characteristics:

1. **FM Library Algorithm**: Uses the division algorithm from the "FM" library rather than traditional long division
2. **Floating-Point Estimation**: Estimates quotient digits using floating-point arithmetic on the first four digits of dividend and divisor
3. **Performance Optimization**: Significantly faster than  but potentially less accurate
4. **Guard Digits**: Computes  extra digits to compensate for potential rounding errors
5. **Approximation Tolerance**: Accepts that some digits may be inexact due to left-propagating rounding errors
6. **Transcendental Use Case**: Primarily intended for transcendental function calculations where approximate results are sufficient

The algorithm uses integer arrays for computation, estimates quotient digits via floating-point division, and includes normalization passes to handle potential overflow and maintain digit accuracy.

## Parameters / Member Variables
- : Dividend NumericVar (input)
- : Divisor NumericVar (input)  
- : NumericVar to store the division result (output)
- : Target fractional digits in the result
- : If true, round at rscale digits; if false, truncate (though truncation is discouraged)

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
  - abs (for absolute value)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE (numeric base constant)
  - DEC_DIGITS (digits per NumericDigit)
  - DIV_GUARD_DIGITS (guard digits for accuracy)
- Called from (representative examples):
  - [div_mod_var](div_mod_var.md)
  - [ln_var](../l/ln_var.md)
  - [log_var](../l/log_var.md)
  - [power_var_int](../p/power_var_int.md)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- **Speed vs. Accuracy Trade-off**: Faster than  but potentially less accurate
- **Truncation Warning**: Using  is discouraged as it may produce results with no significant digits
- **Guard Digits**: Uses  extra precision to mitigate rounding errors
- **Floating-Point Estimation**: Uses double precision to estimate quotient digits from first four dividend/divisor digits
- **Overflow Prevention**: Includes normalization logic to prevent integer overflow during computation
- **Transcendental Functions**: Primarily designed for use in transcendental function calculations
- **FM Library Heritage**: Algorithm derived from the "FM" (presumably "Fast Math") library
- Part of PostgreSQL's arbitrary precision numeric arithmetic system