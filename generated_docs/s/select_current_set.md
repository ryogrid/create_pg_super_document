# select_current_set

## Location
src/backend/executor/nodeAgg.c: 455 - 476

## Overview
Selects the current grouping set in PostgreSQL's aggregate execution engine, affecting the current_set index and curaggcontext memory context.

## Definition
```c
static void select_current_set(AggState *aggstate, int setno, bool is_hash)
```

## Detailed Description
This function manages the state transition for grouping sets in aggregate operations. It sets up the appropriate memory context and tracks the current grouping set number. When processing hash-based aggregation, it uses the shared hash context; otherwise, it selects the specific memory context for the given set number. This is critical for proper memory management during aggregate processing, ensuring that intermediate results are allocated in the correct context.

## Parameters / Member Variables
- `aggstate`: Pointer to AggState structure containing aggregate execution state
- `setno`: The grouping set number to select (0-based index)
- `is_hash`: Boolean indicating whether hash-based aggregation is being used

## Dependencies
- Functions called/Symbols referenced:
  - AggState (struct type)
- Called from (representative examples):
  - initialize_aggregates
  - lookup_hash_entries
  - agg_retrieve_direct
  - agg_fill_hash_table
  - agg_refill_hash_table
  - agg_retrieve_hash_table_in_memory
  - ExecInitAgg
  - ExecReScanAgg

## Notes and Other Information
The function includes a comment noting that changes to this function should also be reflected in ExecAggPlainTransByVal() and ExecAggPlainTransByRef(), indicating tight coupling with the aggregate transition functions. This function is static and internal to the nodeAgg.c file, serving as a utility for managing grouping set state transitions.