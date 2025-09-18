# do_int128_accum

## Location
src/backend/utils/adt/numeric.c: 5534 - 5546

## Overview
Accumulates a new input value into the state structure for 128-bit aggregate functions, updating sum, count, and optionally sum of squares.

## Definition
```c
static void do_int128_accum(Int128AggState *state, int128 newval)
```

## Detailed Description
This function performs the core accumulation operation for 128-bit aggregate functions by adding a new value to the running totals maintained in the state structure. It updates the sum (`sumX`) and count (`N`) for all aggregates, and conditionally updates the sum of squares (`sumX2`) if the `calcSumX2` flag is set in the state. The function handles the mathematical operations required for statistical aggregates like SUM, COUNT, AVG, and variance calculations.

## Parameters / Member Variables
- `state`: Pointer to the `Int128AggState` structure containing the running aggregate values
- `newval`: The new 128-bit integer value to be added to the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - `Int128AggState`: The state structure type being modified
- Called from (representative examples):
  - `int2_accum`: Accumulates smallint values
  - `int4_accum`: Accumulates integer values  
  - `int8_avg_accum`: Accumulates bigint values for average calculation

## Notes and Other Information
- This is a static function, meaning it's only visible within the numeric.c file
- The function performs in-place modification of the state structure
- Sum of squares calculation is conditional based on the `calcSumX2` flag set during state initialization
- Used by various integer accumulation functions across different integer types (int2, int4, int8)
- The 128-bit arithmetic ensures no overflow for reasonable aggregate operations