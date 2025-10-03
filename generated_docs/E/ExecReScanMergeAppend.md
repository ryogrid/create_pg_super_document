# ExecReScanMergeAppend

## Location
[src/backend/executor/nodeMergeAppend.c:340-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergeAppend.c#L340-L377)

## Overview
Resets a MergeAppend node to its initial state for re-execution, handling parameter changes, re-scanning subplans, and reinitializing the binary heap for a fresh merge operation.

## Definition
```c
void ExecReScanMergeAppend(MergeAppendState *node)
```

## Detailed Description
ExecReScanMergeAppend implements the rescan functionality for MergeAppend nodes, which is essential for nested loop joins, subqueries, and other operations that require multiple passes through the same data. The function performs several critical tasks:

**Parameter Change Handling**:
1. **Pruning Parameter Check**: If runtime partition pruning is enabled, checks whether any PARAM_EXEC parameters used in pruning expressions have changed
2. **Subplan Invalidation**: If pruning parameters changed, clears the valid subplans set (ms_valid_subplans) to force re-evaluation of which partitions should be scanned

**Subplan Rescan Management**:
1. **Parameter Propagation**: For each subplan, calls UpdateChangedParamSet to propagate changed parameters down to child nodes
2. **Selective Rescanning**: Only calls ExecReScan on subplans that don't have changed parameters (chgParam == NULL), since subplans with changed parameters will be automatically rescanned on their first ExecProcNode call
3. **Optimization**: This approach avoids redundant rescan operations and ensures proper parameter handling

**State Reset**:
1. **Heap Reset**: Calls binaryheap_reset to clear the binary heap of any existing tuple references
2. **Initialization Flag**: Sets ms_initialized to false, forcing the next ExecMergeAppend call to go through the initialization phase

This design ensures that the MergeAppend node can be efficiently reused within the same query execution for operations like nested loops.

## Parameters / Member Variables
- `*node`: The MergeAppendState containing the merge execution state, subplans, pruning information, and binary heap to be reset
## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md) (checks if two bitmapsets have common members)
  - [bms_free](../b/bms_free.md) (deallocates a bitmapset)
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (propagates parameter changes to child nodes)
  - [ExecReScan](ExecReScan.md) (rescans individual subplans)
  - [binaryheap_reset](../b/binaryheap_reset.md) (clears the binary heap)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (main executor rescan dispatcher)

## Notes and Other Information
- The function is part of the standard PostgreSQL executor node lifecycle and rescan protocol
- Implements an optimization where subplans with changed parameters are not immediately rescanned since they will be rescanned automatically when first accessed
- Runtime partition pruning support allows dynamic re-evaluation of which partitions need to be scanned based on new parameter values
- The ms_initialized flag being set to false ensures that the next execution will rebuild the heap with fresh tuples
- Critical for correctness in nested loop joins where the inner side (containing the MergeAppend) must be rescanned for each outer tuple
- Memory management is handled efficiently - the heap structure is reused rather than deallocated and recreated
- Parameter change detection uses bitmapset operations for efficient comparison of parameter sets

## Simplified Source

```c
void ExecReScanMergeAppend(MergeAppendState *node) {
    int i;

    // Clear valid subplans if pruning parameters changed
    if (node->ms_prune_state &&
        bms_overlap(node->ps.chgParam, node->ms_prune_state->execparamids)) {
        bms_free(node->ms_valid_subplans);
        node->ms_valid_subplans = NULL;
    }

    // Rescan all subplans
    for (i = 0; i < node->ms_nplans; i++) {
        PlanState *subnode = node->mergeplans[i];

        // Propagate parameter changes to subplan
        if (node->ps.chgParam != NULL)
            UpdateChangedParamSet(subnode, node->ps.chgParam);

        // Rescan subplan if no parameters changed
        if (subnode->chgParam == NULL)
            ExecReScan(subnode);
    }

    // Reset merge state
    binaryheap_reset(node->ms_heap);
    node->ms_initialized = false;
}
```