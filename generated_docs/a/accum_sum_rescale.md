# accum_sum_rescale

## Location
src/backend/utils/adt/numeric.c: 12113 - 12201

## Overview
Adjusts the scale and capacity of a NumericSumAccum structure to accommodate a new numeric value that may have different weight, precision, or scale requirements.

## Definition


## Detailed Description
This function dynamically resizes and rescales the accumulator's digit buffers when a new value needs to be added that doesn't fit within the current capacity or scale. It handles three main scenarios: enlarging buffers when the new value has a larger weight (more significant digits), ensuring carry space is available after previous carry propagations, and extending precision when the new value has more digits to the right of the decimal point. The function maintains the invariant that the accumulator always has one extra digit of weight beyond what's needed for the inputs, providing space for carry propagation.

When resizing is needed, the function allocates new buffers and copies existing data to the appropriate positions, ensuring that the accumulated values are preserved correctly.

## Parameters / Member Variables
- : Pointer to the NumericSumAccum structure to be rescaled
- : Pointer to the NumericVar containing the new value that determines rescaling requirements

## Dependencies
- Functions called/Symbols referenced:
  - NumericSumAccum
  - NumericVar
  - palloc0
  - memcpy
  - pfree
- Called from (representative examples):
  - accum_sum_add

## Notes and Other Information
- The function maintains that the accumulator weight is always one larger than needed for inputs to provide carry space
- Both positive and negative digit buffers are resized simultaneously to maintain consistency
- Memory management is handled properly with palloc0/pfree for dynamic buffer allocation
- The function updates both the weight (position of most significant digit) and ndigits (total digit count)
- Scale (dscale) is updated to the maximum of the current and new value's scale
- After rescaling, have_carry_space is set to true since new buffers have reserved space
- The function only performs actual reallocation when necessary, optimizing performance for cases where rescaling isn't needed