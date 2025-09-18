# mul_var

## Location
src/backend/utils/adt/numeric.c: 8685 - 8892

## Overview
Performs multiplication of two NumericVar values using schoolbook multiplication algorithm, with optimizations for performance and precision control.

## Definition


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
  - zero_var (for zero result cases)
  - alloc_var (for result allocation)
  - round_var (for final rounding)
  - strip_var (for removing leading/trailing zeros)
  - palloc0 (for temporary array allocation)
  - pfree (for memory cleanup)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NBASE (numeric base constant)
  - DEC_DIGITS (digits per NumericDigit)
  - MUL_GUARD_DIGITS (guard digits for precision)
- Called from (representative examples):
  - numeric_mul_opt_error
  - numeric_lcm
  - numeric_fac
  - numeric_stddev_internal
  - div_mod_var
  - sqrt_var
  - exp_var
  - ln_var
  - power_var

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Uses an optimized schoolbook multiplication algorithm
- Implements carry propagation normalization to prevent integer overflow
- The inner multiplication loop is designed for auto-vectorization
- Guard digits (MUL_GUARD_DIGITS) help maintain precision during truncation
- Part of PostgreSQL's arbitrary precision numeric arithmetic system
- Memory allocation for temporary arrays is cleaned up automatically