# interval_avg_accum

## Location
[src/backend/utils/adt/timestamp.c:4002-4024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4002-L4024)

## Overview
Transition function for sum() and avg() interval aggregates that accumulates interval values into an aggregation state structure.

## Definition

```c
Datum
interval_avg_accum(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the transition function for PostgreSQL's interval sum() and avg() aggregate functions. It maintains an IntervalAggState structure to accumulate interval values during aggregation. On the first call with a NULL state, it initializes a new aggregation state using makeIntervalAggState. For subsequent calls, it adds non-NULL interval values to the existing state using do_interval_accum. The function follows PostgreSQL's standard aggregate function interface, using the PG_FUNCTION_ARGS macro for parameter handling and returning a Datum pointer to the state.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - ARG 0: IntervalAggState pointer (NULL on first call)
  - ARG 1: Interval value to accumulate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (macro for checking NULL arguments)
  - PG_GETARG_POINTER (macro for retrieving pointer arguments)
  - PG_GETARG_INTERVAL_P (macro for retrieving interval arguments)
  - [makeIntervalAggState](../m/makeIntervalAggState.md) (creates new aggregation state)
  - [do_interval_accum](../d/do_interval_accum.md) (accumulates interval into state)
  - PG_RETURN_POINTER (macro for returning pointer values)
- Called from (representative examples):
  - PostgreSQL aggregate system (registered as transition function)

## Notes and Other Information
- This function is registered with PostgreSQL's aggregate system as the transition function for interval sum/avg operations
- Handles NULL inputs gracefully by skipping accumulation for NULL interval values
- Memory management is handled by PostgreSQL's memory context system through makeIntervalAggState
- Part of PostgreSQL's extensible aggregate function framework