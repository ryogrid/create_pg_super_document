# do_int128_discard

## Location
src/backend/utils/adt/numeric.c: 5547 - 5555

## Overview
Removes an input value from the aggregated state for 128-bit aggregate functions, performing the inverse operation of accumulation.

## Definition
```c
static void do_int128_discard(Int128AggState *state, int128 newval)
```

## Detailed Description
This function performs the inverse accumulation operation for 128-bit aggregate functions by removing a previously accumulated value from the running totals maintained in the state structure. It decrements the sum (`sumX`) and count (`N`) for all aggregates, and conditionally decrements the sum of squares (`sumX2`) if the `calcSumX2` flag is set. This function is essential for implementing sliding window aggregates and inverse aggregate functions that need to remove values from the aggregate state.

## Parameters / Member Variables
- `state`: Pointer to the `Int128AggState` structure containing the running aggregate values
- `newval`: The 128-bit integer value to be removed from the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - `[Int128AggState](../I/Int128AggState.md)`: The state structure type being modified
- Called from (representative examples):
  - `[int2_accum_inv](../i/int2_accum_inv.md)`: Removes smallint values from accumulation
  - `[int4_accum_inv](../i/int4_accum_inv.md)`: Removes integer values from accumulation
  - `[int8_avg_accum_inv](../i/int8_avg_accum_inv.md)`: Removes bigint values from average calculation

## Notes and Other Information
- This is a static function, meaning it's only visible within the numeric.c file
- The function performs in-place modification of the state structure by subtracting values
- Sum of squares removal is conditional based on the `calcSumX2` flag set during state initialization
- Used by inverse aggregate functions for different integer types (int2, int4, int8)
- Essential for sliding window aggregates and moving average calculations
- Assumes the value being discarded was previously accumulated in the state