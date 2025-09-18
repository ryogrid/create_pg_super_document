# process_ordered_aggregate_single

## Location
[src/backend/executor/nodeAgg.c:848-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L848-L948)

## Overview
Processes ordered aggregates with a single input column by completing the sort, reading values in sorted order, and applying the transition function while handling DISTINCT logic.

## Definition
```c
static void process_ordered_aggregate_single(AggState *aggstate,
                                           AggStatePerTrans pertrans,
                                           AggStatePerGroup pergroupstate)
```

## Detailed Description
This function handles the execution phase of DISTINCT or ORDER BY aggregates that have only one input column. It is called after all input values have been entered into the sort object during the scanning phase. The function completes the sort operation, reads out values in sorted order, and applies the aggregate's transition function to each value.

Key optimizations and behaviors:
- Separated from multi-input case for performance: single by-value inputs (like count(distinct id)) run ~300% faster using tuplesort_getdatum
- Implements SQL DISTINCT logic by comparing consecutive sorted values using equality functions
- Manages memory carefully for pass-by-reference types, ensuring proper cleanup of temporary values
- Uses abbreviated comparison when available to optimize DISTINCT checks
- Handles both by-value and by-reference input types appropriately

The function assumes the transition function strictness was already validated during input collection, so it focuses on DISTINCT filtering and value processing.

## Parameters / Member Variables
- `aggstate`: Main aggregate state containing current grouping set information and memory contexts
- `pertrans`: Per-transition state containing sort states, function call info, type information, and equality functions
- `pergroupstate`: Per-group state where transition values are stored and updated

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_performsort
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)  
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - [datumCopy](../d/datumCopy.md)
  - tuplesort_end
- Data types used:
  - [AggState](../A/AggState.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
- Called from (representative examples):
  - [finalize_aggregates](../f/finalize_aggregates.md)

## Notes and Other Information
- Designed specifically for single-column ordered aggregates to maximize performance
- Expects CurrentMemoryContext to be the per-query context when called
- Handles only one grouping set (already set in aggstate->current_set)
- The tuplesort_getdatum path provides significant performance benefits over the general multi-input case
- Properly manages memory for pass-by-reference datums returned by the sort, which are palloc'd in per-query context
- Uses abbreviated values when available for faster DISTINCT comparisons
- Cleans up the sort state by calling tuplesort_end and setting the pointer to NULL when finished