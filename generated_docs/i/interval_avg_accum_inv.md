# interval_avg_accum_inv

## Location
src/backend/utils/adt/timestamp.c: 4149 - 4166

## Overview
Inverse transition function for interval aggregates, used to remove interval values from accumulated state during sliding window aggregations.

## Definition


## Detailed Description
This function implements the inverse transition functionality for interval sum() and avg() aggregates. It is designed to work with sliding window aggregates where values need to be removed from the accumulated state. The function takes an IntervalAggState pointer and an interval value to be removed, then calls the helper function do_interval_discard() to properly update the aggregate state by subtracting the specified interval.

The function includes error handling to ensure it is not called with a NULL state, which would indicate an improper usage scenario.

## Parameters / Member Variables
- : PostgreSQL function argument macro containing:
  - Arg 0: IntervalAggState pointer (aggregate state to modify)
  - Arg 1: Interval pointer (interval value to remove from the aggregate)

## Dependencies
- Functions called/Symbols referenced:
  - IntervalAggState (struct type)
  - PG_ARGISNULL (PostgreSQL macro)
  - PG_GETARG_POINTER (PostgreSQL macro)
  - PG_GETARG_INTERVAL_P (PostgreSQL macro)
  - PG_RETURN_POINTER (PostgreSQL macro)
  - do_interval_discard (helper function)
  - elog (error logging function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's aggregate function infrastructure)

## Notes and Other Information
- This is specifically an inverse transition function, used for sliding window aggregates where values are both added and removed
- The function handles NULL checking for both the state parameter and the interval value to be removed
- Error handling ensures the function fails gracefully if called with invalid state
- Part of PostgreSQL's aggregate function framework for interval data types