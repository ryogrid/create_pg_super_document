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