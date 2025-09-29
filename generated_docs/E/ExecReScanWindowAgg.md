# ExecReScanWindowAgg

## Location
[src/backend/executor/nodeWindowAgg.c:2708-2747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2708-L2747)

## Overview
ExecReScanWindowAgg resets the WindowAgg executor node state to restart execution from the beginning, clearing all cached data and partition state.

## Definition
```c
void ExecReScanWindowAgg(WindowAggState *node)
```

## Detailed Description
ExecReScanWindowAgg reinitializes a WindowAgg executor node for re-execution. The function performs a comprehensive reset by: 1) Setting the execution status back to WINDOWAGG_RUN and marking all_first as true to indicate a fresh start, 2) Releasing all partition-specific resources through release_partition(), 3) Clearing all tuple slots to remove cached tuples, 4) Resetting the expression context arrays for window function values and null indicators, and 5) Conditionally rescanning the outer plan node if no parameter changes are pending. This ensures the WindowAgg node can be re-executed as if it were starting for the first time.

## Parameters / Member Variables
- `node`: Pointer to the WindowAggState structure containing the execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [release_partition](../r/release_partition.md)
  - [ExecClearTuple](ExecClearTuple.md)
  - MemSet
  - [ExecReScan](ExecReScan.md)
  - WINDOWAGG_RUN (status constant)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (general executor rescan mechanism)

## Notes and Other Information
- This function is part of the standard PostgreSQL executor node rescan protocol
- The function clears both mandatory and optional tuple slots (framehead_slot and frametail_slot are conditional)
- Window function values and null indicators are reset using MemSet for efficiency
- The outer plan is only rescanned if no parameter changes are pending (chgParam == NULL)
- Located in src/backend/executor/nodeWindowAgg.c:2708-2747

## Simplified Source

```c
void ExecReScanWindowAgg(WindowAggState *node) {
    PlanState *outerPlan = outerPlanState(node);
    ExprContext *econtext = node->ss.ps.ps_ExprContext;

    // Reset window aggregation state to beginning
    node->status = WINDOWAGG_RUN;
    node->all_first = true;

    // Release partition resources and clear all tuple slots
    release_partition(node);
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    ExecClearTuple(node->first_part_slot);
    ExecClearTuple(node->agg_row_slot);
    ExecClearTuple(node->temp_slot_1);
    ExecClearTuple(node->temp_slot_2);

    // Clear optional frame slots if they exist
    if (node->framehead_slot)
        ExecClearTuple(node->framehead_slot);
    if (node->frametail_slot)
        ExecClearTuple(node->frametail_slot);

    // Reset window function values and null indicators
    MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numfuncs);
    MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numfuncs);

    // Rescan outer plan if no parameter changes pending
    if (outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);
}
```