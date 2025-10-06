# accum_sum_reset

## Location
[src/backend/utils/adt/numeric.c:11976-11991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11976-L11991)

## Overview
Resets a NumericSumAccum accumulator to zero state while preserving allocated digit buffers for efficient reuse in sum operations.

## Definition

```c
static void
accum_sum_reset(NumericSumAccum *accum)
```
## Detailed Description
The  function initializes a NumericSumAccum structure to a zero state by setting the decimal scale to zero and clearing all positive and negative digit arrays. This function is part of PostgreSQL's fast sum accumulator infrastructure, which maintains separate arrays for positive and negative digit contributions to optimize summation performance. Unlike a full deallocation and reallocation, this function preserves the existing digit buffer allocations, making it efficient for reusing accumulators across multiple aggregation operations.

## Parameters / Member Variables
- `*accum`: Pointer to NumericSumAccum structure to be reset to zero state
## Dependencies
- Functions called/Symbols referenced:
  - [NumericSumAccum](../N/NumericSumAccum.md) (accumulator structure type)
- Called from (representative examples):
  - [do_numeric_discard](../d/do_numeric_discard.md) (discarding accumulated numeric values in window functions)

## Notes and Other Information
- Part of the fast sum accumulator system for efficient aggregation operations
- Preserves allocated digit buffers for performance optimization
- Sets dscale (decimal scale) to 0 and clears both pos_digits and neg_digits arrays
- Used in windowing functions and aggregation contexts where accumulators need reset
- More efficient than deallocating and reallocating accumulator structures
- Maintains the ndigits capacity of the accumulator unchanged
- Essential for window function implementations that need to discard old values
- The separate positive and negative digit arrays allow for optimized sum calculations

## Simplified Source

```c
static void accum_sum_reset(NumericSumAccum *accum) {
    // Reset scale to zero
    accum->dscale = 0;

    // Clear all positive and negative digit arrays
    for (int i = 0; i < accum->ndigits; i++) {
        accum->pos_digits[i] = 0;
        accum->neg_digits[i] = 0;
    }
}
```