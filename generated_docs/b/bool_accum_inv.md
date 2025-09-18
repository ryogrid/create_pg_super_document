# bool_accum_inv

## Location
src/backend/utils/adt/bool.c: 349 - 369

## Overview
Removes boolean values from the aggregation state, serving as the inverse transition function for window-based boolean aggregates.

## Definition
```c
Datum bool_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
The bool_accum_inv function acts as the inverse state transition function for boolean aggregation, designed specifically for sliding window operations. It decrements the counters that bool_accum increments, effectively removing a boolean value from the aggregation state. This function is essential for efficient window frame calculations where values slide in and out of the aggregation window. It expects the aggregation state to already exist (created by bool_accum) and will error if called with a NULL state.

## Parameters / Member Variables
- First parameter (PG_GETARG_POINTER(0)): BoolAggState pointer from current aggregation state, must not be NULL
- Second parameter (PG_GETARG_BOOL(1)): Boolean value to remove from aggregation, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [BoolAggState](../B/BoolAggState.md)
  - PG_GETARG_BOOL
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
  - elog
- Called from (representative examples):
  - PostgreSQL window function system (indirectly via pg_proc entries)

## Notes and Other Information
This function is the mathematical inverse of bool_accum and is used exclusively in window function contexts where the aggregation window slides. It decrements both the total count of non-null values and the count of true values when removing a true boolean. The function includes a safety check to prevent calls with NULL state, which would indicate an internal error in the aggregation system. Like bool_accum, it properly handles NULL input values by ignoring them.