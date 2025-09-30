# select_current_set

## Location
[src/backend/executor/nodeAgg.c:455-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L455-L476)

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
  - [AggState](../A/AggState.md) (struct type)
- Called from (representative examples):
  - [initialize_aggregates](../i/initialize_aggregates.md)
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_fill_hash_table](../a/agg_fill_hash_table.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [agg_retrieve_hash_table_in_memory](../a/agg_retrieve_hash_table_in_memory.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [ExecReScanAgg](../E/ExecReScanAgg.md)

## Notes and Other Information
The function includes a comment noting that changes to this function should also be reflected in ExecAggPlainTransByVal() and ExecAggPlainTransByRef(), indicating tight coupling with the aggregate transition functions. This function is static and internal to the nodeAgg.c file, serving as a utility for managing grouping set state transitions.

## Simplified Source

```c
static void select_current_set(AggState *aggstate, int setno, bool is_hash) {
    // Select memory context based on aggregation type
    if (is_hash)
        aggstate->curaggcontext = aggstate->hashcontext;
    else
        aggstate->curaggcontext = aggstate->aggcontexts[setno];

    // Update current grouping set number
    aggstate->current_set = setno;
}
```