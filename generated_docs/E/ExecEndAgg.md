# ExecEndAgg

## Location
[src/backend/executor/nodeAgg.c:4304-4363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4304-L4363)

## Overview
ExecEndAgg performs cleanup and resource deallocation for an aggregate node when execution is finished.

## Definition
```c
void ExecEndAgg(AggState *node)
```

## Detailed Description
This function is responsible for cleaning up all resources associated with an aggregate execution node. It handles multiple aspects of cleanup including parallel worker statistics collection, tuplesort cleanup, hash aggregate spill state reset, memory context deletion, and proper shutdown of expression contexts. The function also ensures that any aggregate shutdown callbacks are properly invoked and recursively ends the outer plan node.

## Parameters / Member Variables
- `node`: Pointer to the AggState structure containing the aggregate execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [tuplesort_end](../t/tuplesort_end.md)
  - [hashagg_reset_spill_state](../h/hashagg_reset_spill_state.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ReScanExprContext](../R/ReScanExprContext.md)
  - outerPlanState
  - [ExecEndNode](ExecEndNode.md)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (src/backend/executor/execProcnode.c:721)

## Notes and Other Information
- Handles parallel worker cleanup by copying statistics back to shared memory for EXPLAIN ANALYZE reporting
- Properly closes all open tuplesorts for both input/output sorts and per-transition sorts
- Resets hash aggregate spill state and deletes hash memory context
- Ensures all expression contexts are properly rescanned to trigger shutdown callbacks
- Part of the standard PostgreSQL executor node cleanup protocol
- Must be called when aggregate processing is complete to prevent resource leaks

## Simplified Source

```c
void
ExecEndAgg(AggState *node)
{
    PlanState *outerPlan;
    int transno;
    int numGroupingSets = Max(node->maxsets, 1);
    int setno;

    // Copy parallel worker statistics back to shared memory
    if (node->shared_info && IsParallelWorker()) {
        AggregateInstrumentation *si;
        si = &node->shared_info->sinstrument[ParallelWorkerNumber];
        si->hash_batches_used = node->hash_batches_used;
        si->hash_disk_used = node->hash_disk_used;
        si->hash_mem_peak = node->hash_mem_peak;
    }

    // Close any open tuplesorts
    if (node->sort_in)
        tuplesort_end(node->sort_in);
    if (node->sort_out)
        tuplesort_end(node->sort_out);

    // Reset hash aggregation spill state
    hashagg_reset_spill_state(node);

    // Delete hash memory context
    if (node->hash_metacxt != NULL) {
        MemoryContextDelete(node->hash_metacxt);
        node->hash_metacxt = NULL;
    }

    // Close per-transition tuplesorts
    for (transno = 0; transno < node->numtrans; transno++) {
        AggStatePerTrans pertrans = &node->pertrans[transno];
        for (setno = 0; setno < numGroupingSets; setno++) {
            if (pertrans->sortstates[setno])
                tuplesort_end(pertrans->sortstates[setno]);
        }
    }

    // Trigger aggregate shutdown callbacks
    for (setno = 0; setno < numGroupingSets; setno++)
        ReScanExprContext(node->aggcontexts[setno]);
    if (node->hashcontext)
        ReScanExprContext(node->hashcontext);

    // Recursively end the outer plan node
    outerPlan = outerPlanState(node);
    ExecEndNode(outerPlan);
}
```