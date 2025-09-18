# interval_avg_combine

## Location
src/backend/utils/adt/timestamp.c: 4025 - 4067

## Overview
Combine function for sum() and avg() interval aggregates that merges two internal aggregate states into the first argument for parallel aggregation.

## Definition


## Detailed Description
This function implements the combine operation for PostgreSQL's parallel aggregation framework for interval sum and avg functions. It takes two IntervalAggState structures and combines them by merging their counts, infinity counters, and summed interval values. The function handles three cases: when either state is NULL (returning the non-NULL state or copying the second to a new first state), and when both states are valid (adding counts and summing finite interval values using finite_interval_pl). This enables PostgreSQL to perform parallel aggregation by combining partial results from different worker processes.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - ARG 0: IntervalAggState pointer (target state to merge into)
  - ARG 1: IntervalAggState pointer (source state to merge from)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (macro for checking NULL arguments)
  - PG_GETARG_POINTER (macro for retrieving pointer arguments)
  - [makeIntervalAggState](../m/makeIntervalAggState.md) (creates new aggregation state)
  - [finite_interval_pl](../f/finite_interval_pl.md) (adds two finite intervals)
  - PG_RETURN_POINTER (macro for returning pointer values)
- Called from (representative examples):
  - PostgreSQL parallel aggregation system (registered as combine function)

## Notes and Other Information
- Essential component of PostgreSQL's parallel query execution for interval aggregates
- Performs field-by-field copying when creating a new state from a NULL first argument
- Handles all three interval components (day, month, time) separately during combination
- Maintains separate counters for positive and negative infinity values
- Only performs finite interval addition when the source state contains finite values (N > 0)