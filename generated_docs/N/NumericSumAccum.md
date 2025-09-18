# NumericSumAccum

## Location
[src/backend/utils/adt/numeric.c:377-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L377-L386)

## Overview
NumericSumAccum is a specialized fast accumulator structure designed for efficiently implementing SUM() and other standard PostgreSQL aggregates that track the sum of multiple numeric input values.

## Definition
```c
typedef struct NumericSumAccum
{
    int         ndigits;
    int         weight;
    int         dscale;
    int         num_uncarried;
    bool        have_carry_space;
    int32      *pos_digits;
    int32      *neg_digits;
} NumericSumAccum;
```

## Detailed Description
NumericSumAccum provides an optimized approach for accumulating sums of numeric values by using 32-bit integers instead of the standard 16-bit NumericDigit format. This allows safe accumulation of up to NBASE-1 values without carry propagation, significantly improving performance for aggregate operations.

The structure maintains separate positive and negative digit buffers (pos_digits and neg_digits) of equal size, weight, and scale. This design simplifies the addition process by avoiding the need to determine add/subtract operations for each new value. The final result is computed by combining these separate sums in accum_sum_final().

The accumulator dynamically resizes when encountering values with larger ndigits or weight, and maintains a reserved carry digit (tracked by have_carry_space) to handle overflow without immediate buffer reallocation. The num_uncarried field tracks how many values have been added without carry propagation.

## Parameters / Member Variables
- `ndigits`: Number of digits in the accumulator buffers
- `weight`: Weight (position) of the first digit in base-NBASE representation
- `dscale`: Display scale (decimal digits after decimal point)
- `num_uncarried`: Count of accumulated values without carry propagation
- `have_carry_space`: Flag indicating if reserved carry digit is available
- `pos_digits`: Buffer for accumulating positive values (32-bit integers)
- `neg_digits`: Buffer for accumulating negative values (32-bit integers)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references, uses standard integer types)
- Called from (representative examples):
  - [NumericAggState](NumericAggState.md) (aggregate state structure)
  - [accum_sum_reset](../a/accum_sum_reset.md) (reset accumulator)
  - [accum_sum_add](../a/accum_sum_add.md) (add value to accumulator)
  - [accum_sum_carry](../a/accum_sum_carry.md) (propagate carries)
  - [accum_sum_rescale](../a/accum_sum_rescale.md) (rescale accumulator)
  - [accum_sum_final](../a/accum_sum_final.md) (compute final result)
  - [accum_sum_copy](../a/accum_sum_copy.md) (copy accumulator)
  - [accum_sum_combine](../a/accum_sum_combine.md) (combine accumulators)

## Notes and Other Information
- Optimized for SUM() aggregates and similar operations requiring accumulation
- Uses 32-bit arithmetic to reduce carry propagation overhead
- Separates positive and negative values for computational efficiency
- Automatically resizes to accommodate larger input values
- Does not handle NaN values (must be handled at higher level)
- Initialized by zeroing all fields
- Critical component of PostgreSQL's high-performance numeric aggregation system
- Designed to minimize memory allocations and arithmetic operations during aggregation