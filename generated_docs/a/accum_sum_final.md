# accum_sum_final

## Location
src/backend/utils/adt/numeric.c: 12202 - 12252

## Overview
Finalizes the accumulator computation by performing final carry propagation and converting the accumulated positive and negative sums into a single NumericVar result.

## Definition


## Detailed Description
This function completes the accumulation process by first performing any pending carry propagation, then creating separate NumericVar structures for the positive and negative accumulated values, and finally combining them through subtraction to produce the final result. The function handles the conversion from the internal int32 digit representation used in the accumulator to the int16 NumericDigit representation used in NumericVar structures. It also strips leading and trailing zeros from the final result to maintain canonical numeric representation.

The function is designed to work across memory contexts, allowing callers to be in different memory contexts than the accumulator itself.

## Parameters / Member Variables
- : Pointer to the NumericSumAccum structure containing the accumulated values
- : Pointer to the NumericVar where the final computed result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - accum_sum_carry
  - set_var_from_var
  - init_var
  - digitbuf_alloc
  - add_var
  - strip_var
  - NumericSumAccum
  - NumericVar
  - NUMERIC_POS
  - NUMERIC_NEG
  - NBASE
  - const_zero
- Called from (representative examples):
  - numeric_avg_serialize
  - numeric_serialize
  - numeric_poly_serialize
  - int8_avg_serialize
  - numeric_avg
  - numeric_sum
  - numeric_stddev_internal
  - accum_sum_combine

## Notes and Other Information
- The function returns zero (via const_zero) if the accumulator has no digits (ndigits == 0)
- Final carry propagation is performed to ensure all digits are in proper NBASE range before conversion
- Separate NumericVar structures are created for positive and negative sums with identical weight, scale, and digit count
- The int32 accumulator digits are safely converted to int16 NumericDigit format with assertions to verify they're within NBASE
- The final result is the sum of the positive NumericVar and negative NumericVar (which effectively performs subtraction due to the negative sign)
- Leading and trailing zeros are stripped to maintain canonical numeric representation
- Unlike other accumulator functions, this one doesn't require the caller to be in the accumulator's memory context