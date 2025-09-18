# accum_sum_copy

## Location
src/backend/utils/adt/numeric.c: 12253 - 12269

## Overview
Creates a deep copy of a NumericSumAccum structure, duplicating all digit arrays and metadata to enable independent manipulation of accumulator state.

## Definition


## Detailed Description
This function performs a complete deep copy of a NumericSumAccum structure from source to destination. It allocates new memory buffers for both the positive and negative digit arrays and copies all the digit data along with the accumulator's metadata (number of uncarried values, digit count, weight, and decimal scale). The destination accumulator is assumed to be uninitialized, and the function does not attempt to free any existing memory in the destination structure.

This function is essential for operations that need to preserve the original accumulator state while creating a working copy, such as in combine operations for parallel aggregation.

## Parameters / Member Variables
- : Pointer to the uninitialized destination NumericSumAccum structure
- : Pointer to the source NumericSumAccum structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [NumericSumAccum](../N/NumericSumAccum.md)
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - [numeric_combine](../n/numeric_combine.md)
  - [numeric_avg_combine](../n/numeric_avg_combine.md)
  - [numeric_poly_combine](../n/numeric_poly_combine.md)
  - [int8_avg_combine](../i/int8_avg_combine.md)

## Notes and Other Information
- The destination accumulator must be uninitialized; no cleanup of existing data is performed
- Both positive and negative digit arrays are allocated and copied completely
- All metadata fields are copied: num_uncarried, ndigits, weight, and dscale
- Memory is allocated using palloc rather than palloc0, since the data will be immediately overwritten by memcpy
- The have_carry_space field is not explicitly copied, which may indicate it's recalculated when needed
- This function is commonly used in combine operations for parallel aggregation where multiple partial results need to be merged