# do_numeric_discard

## Location
src/backend/utils/adt/numeric.c: 4943 - 5035

## Overview
A static helper function that attempts to remove an input value from the aggregated state in PostgreSQL's numeric aggregate operations, primarily used for inverse aggregate functions in window operations.

## Definition


## Detailed Description
This function implements the core logic for removing a previously aggregated numeric value from an aggregate state, which is essential for sliding window aggregate operations. The function handles the complex task of maintaining aggregate state consistency when values are removed, including proper management of decimal scale tracking and special numeric values (NaN, positive/negative infinity).

The function must handle several challenging scenarios:
- **Scale Management**: Tracks the maximum decimal scale (dscale) of all aggregated values. If removing a value would make it impossible to determine the correct scale for the remaining sum, the function fails and forces re-computation.
- **Special Values**: Separately counts and manages NaN, positive infinity, and negative infinity values.
- **Sum Maintenance**: Updates both the sum (sumX) and sum of squares (sumX2) by subtracting the removed value.
- **Memory Context**: Performs calculations in appropriate memory contexts to ensure proper memory management.

The function may return false to indicate that the removal cannot be performed cleanly, forcing the aggregate to be recalculated from scratch.

## Parameters / Member Variables
- : Pointer to NumericAggState containing the current aggregate state (sum, count, scale information, special value counts)
- : The Numeric value to be removed from the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL
  - NUMERIC_IS_PINF  
  - NUMERIC_IS_NINF
  - [init_var_from_num](../i/init_var_from_num.md)
  - init_var
  - [mul_var](../m/mul_var.md)
  - [accum_sum_add](../a/accum_sum_add.md)
  - [accum_sum_reset](../a/accum_sum_reset.md)
  - NUMERIC_POS
  - NUMERIC_NEG
- Called from (representative examples):
  - [numeric_accum_inv](../n/numeric_accum_inv.md)
  - [int2_accum_inv](../i/int2_accum_inv.md)
  - [int4_accum_inv](../i/int4_accum_inv.md)
  - [int8_accum_inv](../i/int8_accum_inv.md)
  - [int8_avg_accum_inv](../i/int8_avg_accum_inv.md)

## Notes and Other Information
- This function is critical for implementing inverse aggregate functions that support moving window operations
- The scale management logic is particularly complex because PostgreSQL must maintain precision information accurately
- Failure to remove a value (returning false) triggers a complete re-aggregation from scratch, which is expensive but ensures correctness
- The function handles both sum and sum-of-squares calculations when variance/standard deviation operations are needed
- Special numeric values (NaN, infinity) are tracked separately from regular numeric values to maintain IEEE 754 compliance