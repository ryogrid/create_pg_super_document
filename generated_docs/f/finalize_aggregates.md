# finalize_aggregates

## Location
[src/backend/executor/nodeAgg.c:1294-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1294-L1370)

## Overview
Computes the final value of all aggregates for one group by processing ordered/distinct aggregates and running final functions, storing results in the output expression context.

## Definition
```c
static void finalize_aggregates(AggState *aggstate,
                               AggStatePerAgg peraggs,
                               AggStatePerGroup pergroup)
```

## Detailed Description
This function performs the complete finalization of all aggregates for a single group. It operates in two main phases: first, it processes any DISTINCT and/or ORDER BY aggregates by sorting their inputs and running transition functions, then it runs the final functions for all aggregates. The function handles both regular aggregates (using finalize_aggregate) and partial aggregates (using finalize_partialaggregate) depending on the aggregate split mode. It also manages cleanup of distinct value tracking state between groups.

## Parameters / Member Variables
- `aggstate`: The overall aggregate execution state containing global configuration
- `peraggs`: Array of per-aggregate state information
- `pergroup`: Array of per-group state containing transition values for the current group

## Dependencies
- Functions called/Symbols referenced:
  - [process_ordered_aggregate_single](../p/process_ordered_aggregate_single.md)
  - [process_ordered_aggregate_multi](../p/process_ordered_aggregate_multi.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [finalize_partialaggregate](finalize_partialaggregate.md)
  - [finalize_aggregate](finalize_aggregate.md)
  - DO_AGGSPLIT_SKIPFINAL
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_retrieve_hash_table_in_memory](../a/agg_retrieve_hash_table_in_memory.md)

## Notes and Other Information
- Handles only one grouping set at a time; caller must select the appropriate grouping set
- Caller is responsible for adjusting the pergroup parameter to point to current set's transition values
- Results are stored directly in the expression context's aggvalues/aggnulls arrays
- Ordered aggregates are not compatible with AGG_HASHED or AGG_MIXED strategies
- Manages memory cleanup for distinct aggregates, including freeing non-byval datums
- Supports both single-column and multi-column distinct/ordered aggregates
- The aggregate split mode determines whether to use partial or full finalization
- Critical component in the aggregate execution pipeline that bridges transition state to final results