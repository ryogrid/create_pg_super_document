# accum_sum_add

## Location
[src/backend/utils/adt/numeric.c:11992-12039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11992-L12039)

## Overview
Accumulates a new numeric value into a NumericSumAccum structure, which is used for efficient computation of sums and averages over large sets of numeric values.

## Definition

```c
static void
accum_sum_add(NumericSumAccum *accum, const NumericVar *val)
```
## Detailed Description
This function adds a new numeric value to an accumulator structure that maintains separate arrays for positive and negative digits to enable efficient summation. The function performs carry propagation when necessary to prevent overflow, rescales the accumulator to accommodate the new value's weight and scale, and then adds the digits to the appropriate accumulator array based on the value's sign.

The function is designed to handle very large sums efficiently by deferring carry propagation until a threshold (NBASE - 1 accumulated values) is reached, which minimizes the computational overhead while preventing overflow.

## Parameters / Member Variables
- `*accum`: Pointer to the NumericSumAccum structure that maintains the running sum
- `*val`: Pointer to the NumericVar containing the value to be added to the accumulator
## Dependencies
- Functions called/Symbols referenced:
  - [accum_sum_carry](accum_sum_carry.md)
  - [accum_sum_rescale](accum_sum_rescale.md)
  - [NumericSumAccum](../N/NumericSumAccum.md)
  - [NumericVar](../N/NumericVar.md)
  - NumericDigit
  - NBASE
  - NUMERIC_POS
- Called from (representative examples):
  - [do_numeric_accum](../d/do_numeric_accum.md)
  - [do_numeric_discard](../d/do_numeric_discard.md)
  - [numeric_avg_deserialize](../n/numeric_avg_deserialize.md)
  - [numeric_deserialize](../n/numeric_deserialize.md)
  - [numeric_poly_deserialize](../n/numeric_poly_deserialize.md)
  - [int8_avg_deserialize](../i/int8_avg_deserialize.md)
  - [numeric_poly_stddev_internal](../n/numeric_poly_stddev_internal.md)
  - [accum_sum_combine](accum_sum_combine.md)

## Notes and Other Information
- The function maintains separate positive and negative digit arrays to optimize addition operations
- Carry propagation is performed lazily when num_uncarried reaches NBASE - 1 to balance performance and overflow prevention
- The accumulator is automatically rescaled to accommodate values with different weights and scales
- This is a core component of PostgreSQL's numeric aggregation system, enabling efficient computation of SUM, AVG, and statistical functions