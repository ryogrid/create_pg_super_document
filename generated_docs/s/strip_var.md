# strip_var

## Location
src/backend/utils/adt/numeric.c: 11935 - 11975

## Overview
Removes leading and trailing zero digits from a NumericVar to maintain canonical representation and optimize storage efficiency.

## Definition


## Detailed Description
The  function normalizes a NumericVar by removing unnecessary leading and trailing zero digits that don't contribute to the numeric value. Leading zeros are removed by advancing the digits pointer and decreasing the weight accordingly. Trailing zeros are removed by reducing the ndigits count. When all digits are removed (resulting in a zero value), the function normalizes the sign to positive and sets the weight to zero. This function is essential for maintaining the canonical form of numeric values and preventing unnecessary storage overhead.

## Parameters / Member Variables
- : Pointer to NumericVar structure to be normalized (modified in place)

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type for individual digits)
  - NUMERIC_POS (positive sign constant for zero normalization)
- Called from (representative examples):
  - set_var_from_str (parsing numeric strings)
  - numericvar_to_int64, numericvar_to_uint64, numericvar_to_int128 (numeric conversions)
  - mul_var (multiplication operations)
  - div_var, div_var_fast, div_var_int, div_var_int64 (division operations)
  - sqrt_var (square root calculation)
  - random_var (random number generation)
  - add_abs, sub_abs (absolute value arithmetic)
  - accum_sum_final (sum accumulation finalization)

## Notes and Other Information
- Critical for maintaining canonical representation of numeric values
- Adjusts both digits pointer and weight when removing leading zeros
- Essential for preventing storage bloat from unnecessary zero digits
- Automatically normalizes zero values to have positive sign and zero weight
- Called after most arithmetic operations to ensure clean results
- Does not allocate or free memory - only adjusts pointers and counts
- Preserves the original digit buffer while potentially changing the active digit range
- Part of the fundamental numeric value maintenance infrastructure in PostgreSQL