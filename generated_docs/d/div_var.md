# div_var

## Location
src/backend/utils/adt/numeric.c: 8893 - 9200

## Overview
Performs division of two NumericVar values using long division algorithm with optimizations for short divisors and precise control over result precision and rounding.

## Definition


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
  - div_var_int (for short integer divisors)
  - div_var_int64 (for 64-bit integer divisors on 128-bit platforms)
  - zero_var (for zero dividend case)
  - alloc_var (for result allocation)
  - round_var (for rounding result)
  - trunc_var (for truncating result)
  - strip_var (for removing leading/trailing zeros)
  - palloc0/pfree (for memory management)
  - memcpy (for copying digit arrays)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE/HALF_NBASE (numeric base constants)
  - DEC_DIGITS (digits per NumericDigit)
- Called from (representative examples):
  - numeric_div_opt_error
  - numeric_div_trunc
  - numeric_lcm
  - numeric_stddev_internal
  - mod_var
  - power_var_int
  - get_str_from_var_sci

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Implements Knuth's Algorithm 4.3.1D for long division
- Automatically delegates to optimized short division routines when possible
- The algorithm ensures the first divisor digit is >= NBASE/2 for numerical stability
- Supports both rounding and truncation modes for result precision
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Uses temporary memory allocation that is cleaned up automatically
- The quotient estimation and correction steps ensure mathematical accuracy