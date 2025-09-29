# ExecReScanGatherMerge

## Location
[src/backend/executor/nodeGatherMerge.c:334-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L334-L387)

## Overview
Prepares a GatherMerge node to re-scan its result by gracefully shutting down existing workers, cleaning up state, and setting up parameters for a fresh scan.

## Definition
```c
void ExecReScanGatherMerge(GatherMergeState *node)
```

## Detailed Description
ExecReScanGatherMerge implements the rescan functionality for GatherMerge nodes in PostgreSQL's parallel query execution. When a rescan is requested, this function ensures a clean restart by:

1. Gracefully shutting down any existing parallel workers to prevent resource leaks
2. Clearing any cached tuples to avoid memory leaks across rescans
3. Resetting initialization flags to force rebuilding of shared state on the next execution
4. Managing parameter changes that affect the child plan's execution
5. Conditionally rescanning the outer (child) plan based on parameter change detection

The function handles the complexity of parallel execution rescans by ensuring that both shared state (managed by workers) and local state are properly reset. It uses a rescan parameter mechanism to optimize cases where the overall rowset hasn't changed but the leader process's subset might differ.

## Parameters / Member Variables
- `node`: The GatherMergeState containing the execution state for this GatherMerge node, including worker management and tuple storage

## Dependencies
- Functions called/Symbols referenced:
  - [ExecShutdownGatherMergeWorkers](ExecShutdownGatherMergeWorkers.md)
  - [gather_merge_clear_tuples](../g/gather_merge_clear_tuples.md)
  - outerPlanState
  - [bms_add_member](../b/bms_add_member.md)
  - [ExecReScan](ExecReScan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic rescan dispatcher)

## Notes and Other Information
- The function carefully manages the interaction between parallel workers and the leader process during rescans
- Uses a rescan_param mechanism to optimize cases where only the leader's subset of rows changes
- Ensures proper ordering: shared state reset (ReInitializeDSM) should happen before local state reset (ReScan)
- The chgParam mechanism prevents child nodes from making incorrect optimizations based on assumptions about unchanging rowsets
- Memory management is critical - all unused tuples must be freed to prevent leaks across multiple rescans

## Simplified Source

```c
void ExecReScanGatherMerge(GatherMergeState *node) {
    GatherMerge *gm = (GatherMerge *) node->ps.plan;
    PlanState *outerPlan = outerPlanState(node);

    // Step 1: Clean shutdown of existing parallel workers
    ExecShutdownGatherMergeWorkers(node);

    // Step 2: Free cached tuples to prevent memory leaks
    gather_merge_clear_tuples(node);

    // Step 3: Reset initialization flags for fresh start
    node->initialized = false;
    node->gm_initialized = false;

    // Step 4: Handle parameter changes for child plan optimization
    if (gm->rescan_param >= 0) {
        outerPlan->chgParam = bms_add_member(outerPlan->chgParam, gm->rescan_param);
    }

    // Step 5: Rescan child plan if no parameter changes detected
    if (outerPlan->chgParam == NULL) {
        ExecReScan(outerPlan);
    }
}
```