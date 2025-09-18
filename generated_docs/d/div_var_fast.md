# div_var_fast

## Location
src/backend/utils/adt/numeric.c: 9201 - 9564

## Overview
A fast division algorithm for NumericVar values using floating-point estimation and the FM library approach, optimized for transcendental function calculations where approximate results are acceptable.

## Definition


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
  - div_var_int (for short integer divisors)
  - div_var_int64 (for 64-bit integer divisors on 128-bit platforms)
  - zero_var (for zero dividend case)
  - alloc_var (for result allocation)
  - round_var (for rounding result)
  - trunc_var (for truncating result)
  - strip_var (for removing leading/trailing zeros)
  - palloc0/pfree (for memory management)
  - abs (for absolute value)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE (numeric base constant)
  - DEC_DIGITS (digits per NumericDigit)
  - DIV_GUARD_DIGITS (guard digits for accuracy)
- Called from (representative examples):
  - div_mod_var
  - ln_var
  - log_var
  - power_var_int

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