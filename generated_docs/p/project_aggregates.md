# project_aggregates

## Location
src/backend/executor/nodeAgg.c: 1371 - 1396

## Overview
Projects the result of a group whose aggregates have already been calculated, applying the HAVING clause and forming the final output tuple.

## Definition
```c
static TupleTableSlot *project_aggregates(AggState *aggstate)
```

## Detailed Description
This function performs the final step in aggregate processing by projecting the results of a completed group calculation. It first evaluates the HAVING clause (qual) to determine if the group should be included in the output. If the group passes the HAVING filter, it uses the projection information to form the final output tuple combining aggregate results with any non-aggregate expressions. If the group is filtered out by the HAVING clause, it increments the filtered tuple counter and returns NULL.

## Parameters / Member Variables
- `aggstate`: The aggregate execution state containing expression context and projection information

## Dependencies
- Functions called/Symbols referenced:
  - ExecQual
  - ExecProject
  - InstrCountFiltered1
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_retrieve_hash_table_in_memory](../a/agg_retrieve_hash_table_in_memory.md)

## Notes and Other Information
- Assumes that finalize_aggregates has already been called to compute aggregate values
- The aggregate results are available in the expression context's aggvalues/aggnulls arrays
- Uses the plan state's qual for HAVING clause evaluation
- Uses the plan state's projection info to form the final tuple
- Returns NULL when groups are suppressed by the HAVING clause
- Includes instrumentation to count filtered tuples for query planning feedback
- The final step in the aggregate pipeline before returning results to the caller
- Works with both the representative input tuple and computed aggregate values to form complete output rows