# do_interval_accum

## Location
src/backend/utils/adt/timestamp.c: 3948 - 3970

## Overview
Accumulates a new interval value into the aggregate state for interval aggregate functions, handling both finite and infinite interval values separately.

## Definition
```c
static void do_interval_accum(IntervalAggState *state, Interval *newval)
```

## Detailed Description
This static function is the core accumulation logic for interval aggregate functions such as AVG and SUM. It processes each new interval value and updates the aggregate state accordingly. The function distinguishes between finite intervals and infinite intervals (-infinity and +infinity), treating them separately to maintain mathematical correctness in aggregate calculations.

For infinite intervals, the function maintains separate counters (nInfcount for negative infinity, pInfcount for positive infinity) without affecting the count of finite values (N). For finite intervals, it adds the value to the running sum using finite_interval_pl and increments the count of finite values. This approach ensures that infinite values don't corrupt the aggregate calculations while still being properly accounted for.

## Parameters / Member Variables
- `state`: Pointer to the IntervalAggState structure maintaining the aggregate state
- `newval`: Pointer to the new Interval value to be accumulated

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_IS_NOBEGIN
  - INTERVAL_IS_NOEND
  - [finite_interval_pl](../f/finite_interval_pl.md)
  - [IntervalAggState](../I/IntervalAggState.md) (type)
  - Interval (type)
- Called from:
  - [interval_avg_accum](../i/interval_avg_accum.md) (in src/backend/utils/adt/timestamp.c:4013)

## Notes and Other Information
- Static function used internally by interval aggregate functions
- Handles infinite intervals separately from finite values to maintain mathematical correctness
- Uses finite_interval_pl for safe addition of finite intervals
- Maintains separate counters for positive and negative infinity occurrences
- Does not increment N (finite value count) for infinite inputs
- Essential for proper functioning of interval AVG and SUM aggregates