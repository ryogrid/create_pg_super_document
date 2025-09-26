# array_agg_finalfn

## Location
[src/backend/utils/adt/array_userfuncs.c:822-856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L822-L856)

## Overview
Finalizes the array_agg() aggregate by converting the accumulated ArrayBuildState into a PostgreSQL array result.

## Definition

```c
Datum
array_agg_finalfn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the final step in the array_agg() aggregate process, converting the accumulated ArrayBuildState structure into an actual PostgreSQL array. It creates a one-dimensional array from the collected elements, setting appropriate dimensions and lower bounds. The function handles the NULL case by returning NULL if no input values were provided during aggregation.

The function deliberately does not release the ArrayBuildState memory, as aggregate final functions may be re-executed in certain scenarios. Memory cleanup is instead handled by nodeAgg.c when it's safe to reset the aggregate context.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : ArrayBuildState pointer containing accumulated elements (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
  - [makeMdArrayResult](../m/makeMdArrayResult.md)
  - PG_RETURN_DATUM
  - PG_ARGISNULL
  - PG_GETARG_POINTER
- Called from (representative examples):
  - PostgreSQL aggregate framework (internal)
  - array_agg() aggregate function execution

## Notes and Other Information
- Final function in the array_agg() aggregate chain
- Creates a one-dimensional array with lower bound of 1 (following SQL standard)
- Returns NULL if the aggregate processed no input values
- Does not free the ArrayBuildState to allow for potential re-execution
- Memory management is delegated to the aggregate execution context
- Essential component that produces the final array result visible to SQL users
- Works with both serial and parallel aggregate execution modes