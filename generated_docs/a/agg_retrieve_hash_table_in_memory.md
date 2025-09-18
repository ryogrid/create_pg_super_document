# agg_retrieve_hash_table_in_memory

## Location
[src/backend/executor/nodeAgg.c:2771-2893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2771-L2893)

## Overview
Retrieves groups from the in-memory hash tables during hash aggregation without considering any spilled tuples, iterating through all grouping sets.

## Definition


## Detailed Description
This function is responsible for retrieving aggregated groups from in-memory hash tables during the hash aggregation process. It operates in a loop to scan through hash table entries across all grouping sets, finalizing aggregates for each group and projecting the results. The function handles multiple grouping sets by switching between them when one hash table is exhausted.

The function performs several key operations:
1. Scans the current hash table using 
2. When a hash table is exhausted, switches to the next grouping set
3. Transforms representative tuples back into the proper column format
4. Finalizes aggregates for each group using 
5. Projects the final result using 
6. Applies qualification checks before returning results

## Parameters / Member Variables
- : The aggregate node's execution state containing hash tables, grouping information, and per-aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - ScanTupleHashTable
  - [select_current_set](../s/select_current_set.md)
  - ResetTupleHashIterator
  - ResetExprContext
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - slot_getallattrs
  - ExecClearTuple
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [prepare_projection_slot](../p/prepare_projection_slot.md)
  - [finalize_aggregates](../f/finalize_aggregates.md)
  - [project_aggregates](../p/project_aggregates.md)
- Called from (representative examples):
  - [agg_retrieve_hash_table](agg_retrieve_hash_table.md)

## Notes and Other Information
- This function only handles in-memory hash tables and does not process spilled tuples
- It supports multiple grouping sets by iterating through all available hash tables
- The function uses CHECK_FOR_INTERRUPTS() to allow query cancellation during long-running aggregations
- Memory context management is handled carefully to avoid premature cleanup of aggregate state
- Returns NULL when all groups from all grouping sets have been processed