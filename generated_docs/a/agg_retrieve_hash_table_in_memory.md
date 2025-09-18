# agg_retrieve_hash_table_in_memory

## Location
src/backend/executor/nodeAgg.c: 2771 - 2893

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
  - select_current_set
  - ResetTupleHashIterator
  - ResetExprContext
  - ExecStoreMinimalTuple
  - slot_getallattrs
  - ExecClearTuple
  - ExecStoreVirtualTuple
  - prepare_projection_slot
  - finalize_aggregates
  - project_aggregates
- Called from (representative examples):
  - agg_retrieve_hash_table

## Notes and Other Information
- This function only handles in-memory hash tables and does not process spilled tuples
- It supports multiple grouping sets by iterating through all available hash tables
- The function uses CHECK_FOR_INTERRUPTS() to allow query cancellation during long-running aggregations
- Memory context management is handled carefully to avoid premature cleanup of aggregate state
- Returns NULL when all groups from all grouping sets have been processed