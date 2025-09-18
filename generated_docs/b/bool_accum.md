# bool_accum

## Location
[src/backend/utils/adt/bool.c:328-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L328-L348)

## Overview  
Accumulates boolean values for aggregate functions, maintaining counts of total non-null values and true values.

## Definition
```c
Datum bool_accum(PG_FUNCTION_ARGS)
```

## Detailed Description
The bool_accum function serves as the state transition function for PostgreSQL's boolean aggregate functions like EVERY/ALL and SOME/ANY. It processes each input boolean value during aggregation, updating the internal state counters. On the first call, it creates the aggregation state using makeBoolAggState. For subsequent calls, it increments the count of non-null values and, if the input value is true, increments the count of true values. The function handles NULL input values by ignoring them (standard SQL aggregate behavior).

## Parameters / Member Variables
- First parameter (PG_GETARG_POINTER(0)): BoolAggState pointer from previous accumulation, or NULL on first call
- Second parameter (PG_GETARG_BOOL(1)): Boolean value to accumulate, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [makeBoolAggState](../m/makeBoolAggState.md)
  - [BoolAggState](../B/BoolAggState.md)
  - PG_GETARG_BOOL
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL aggregate system (indirectly via pg_proc entries)

## Notes and Other Information
This function follows the standard PostgreSQL aggregate function protocol, taking the current state as the first argument and the new value as the second argument. It's designed to be used with window functions and supports both regular aggregation and moving window aggregation scenarios. The function ensures proper NULL handling according to SQL standards where NULLs are ignored in boolean aggregation.