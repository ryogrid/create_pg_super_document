# ExecReScanAppend

## Location
[src/backend/executor/nodeAppend.c:406-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L406-L483)

## Overview
Resets an Append node to restart execution from the beginning, handling parameter changes, partition pruning state, and asynchronous execution cleanup.

## Definition

```c
void
ExecReScanAppend(AppendState *node)
```
## Detailed Description
ExecReScanAppend is the rescan function for PostgreSQL's Append node executor, responsible for resetting the node's execution state so it can be re-executed from the beginning. This is commonly needed when the Append node is part of a nested loop or when parameter values change that affect subplan selection.

The function handles several critical aspects:

1. **Parameter Change Detection**: Checks if any PARAM_EXEC parameters used in partition pruning have changed, requiring recomputation of valid subplans
2. **Pruning State Reset**: Clears cached valid subplan information when parameters change
3. **Subplan Propagation**: Propagates parameter changes to all subplans using UpdateChangedParamSet
4. **Selective Rescanning**: Only rescans subplans that don't have parameter changes (others will be rescanned automatically on next execution)
5. **Async State Reset**: Cleans up all asynchronous execution state including pending requests and results
6. **Execution State Reset**: Returns the node to its initial state ready for fresh execution

## Parameters / Member Variables
- : The AppendState containing the execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md) (for checking parameter intersection)
  - [bms_free](../b/bms_free.md) (for cleaning up bitmap sets)
  - [bms_next_member](../b/bms_next_member.md) (for iterating async plans)
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (for parameter change propagation)
  - [ExecReScan](ExecReScan.md) (for recursive subplan rescanning)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (main executor rescan dispatcher)

## Notes and Other Information
- The function intelligently avoids double-rescanning subplans that will be rescanned automatically due to parameter changes
- Partition pruning state is only reset when relevant parameters actually change, preserving performance
- Async execution state is completely reset to ensure clean restart
- The function uses UpdateChangedParamSet (one of the processed symbols) to propagate parameter changes efficiently
- Parameter change detection uses bitmap overlap operations for efficiency
- The rescan operation prepares the node for potential re-pruning of partitions if parameters affecting pruning have changed

## Simplified Source

```c
void ExecReScanAppend(AppendState *node) {
    // Reset partition pruning state if parameters changed
    if (node->as_prune_state &&
        bms_overlap(node->ps.chgParam, node->as_prune_state->execparamids)) {
        node->as_valid_subplans_identified = false;
        bms_free(node->as_valid_subplans);
        node->as_valid_subplans = NULL;
        bms_free(node->as_valid_asyncplans);
        node->as_valid_asyncplans = NULL;
    }

    // Rescan all subplans, propagating parameter changes
    for (int i = 0; i < node->as_nplans; i++) {
        PlanState *subnode = node->appendplans[i];

        // Propagate parameter changes to subplan
        if (node->ps.chgParam != NULL) {
            UpdateChangedParamSet(subnode, node->ps.chgParam);
        }

        // Rescan subplan if it has no pending parameter changes
        if (subnode->chgParam == NULL) {
            ExecReScan(subnode);
        }
    }

    // Reset asynchronous execution state
    if (node->as_nasyncplans > 0) {
        // Reset all async request states
        int i = -1;
        while ((i = bms_next_member(node->as_asyncplans, i)) >= 0) {
            AsyncRequest *areq = node->as_asyncrequests[i];
            areq->callback_pending = false;
            areq->request_complete = false;
            areq->result = NULL;
        }

        // Clear async counters and request bitmap
        node->as_nasyncresults = 0;
        node->as_nasyncremain = 0;
        bms_free(node->as_needrequest);
        node->as_needrequest = NULL;
    }

    // Reset execution state to beginning
    node->as_whichplan = INVALID_SUBPLAN_INDEX;
    node->as_syncdone = false;
    node->as_begun = false;
}
```