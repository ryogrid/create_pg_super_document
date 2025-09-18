# accum_sum_carry

## Location
src/backend/utils/adt/numeric.c: 12040 - 12112

## Overview
Propagates carries in both positive and negative digit arrays of a NumericSumAccum structure to maintain proper numeric representation after accumulating values.

## Definition


## Detailed Description
This function performs carry propagation on the accumulated digits in both the positive and negative digit arrays within a NumericSumAccum structure. When digits exceed the base value (NBASE), the excess is carried to the next higher-order digit position. The function processes digits from least significant to most significant, ensuring that each digit remains within the valid range [0, NBASE-1]. It also tracks whether the reserved carry space has been used, which is important for managing buffer overflow in subsequent operations.

The function maintains the invariant that the accumulator always has space for one extra digit before carry propagation, preventing buffer overflow issues.

## Parameters / Member Variables
- : Pointer to the NumericSumAccum structure containing the digit arrays that need carry propagation

## Dependencies
- Functions called/Symbols referenced:
  - [NumericSumAccum](../N/NumericSumAccum.md)
  - NBASE
- Called from (representative examples):
  - [accum_sum_add](accum_sum_add.md)
  - [accum_sum_final](accum_sum_final.md)

## Notes and Other Information
- The function only performs carry propagation if num_uncarried > 0, optimizing performance by avoiding unnecessary work
- Both positive and negative digit arrays are processed separately using identical carry propagation logic
- The have_carry_space flag is updated to track whether the reserved space for carry has been consumed
- The weight of the accumulator is maintained to be one larger than needed before carrying to ensure sufficient space
- After completion, num_uncarried is reset to 0 to indicate that carry propagation is up to date