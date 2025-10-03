# ExecParallelReInitializeDSM

## Location
[src/backend/executor/execParallel.c:953-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L953-L1021)

## Overview
Traverses the entire plan tree to reinitialize Dynamic Shared Memory (DSM) state for all parallel-aware nodes when restarting parallel query execution.

## Definition
```c
static bool ExecParallelReInitializeDSM(PlanState *planstate, ParallelContext *pcxt)
```

## Detailed Description
ExecParallelReInitializeDSM is a recursive function that walks through a plan tree and calls the appropriate reinitialization functions for each parallel-aware node type that uses Dynamic Shared Memory. This function is essential when restarting parallel query execution, as it ensures that all shared memory state across different types of scan and join nodes is properly reset to their initial state.

The function uses a switch statement to identify specific node types and calls their corresponding reinitialization functions only if the node is marked as parallel-aware. It handles various scan types (sequential, index, foreign, bitmap heap), join operations (hash join), utility nodes (append, custom scan), and notes that some nodes (hash, sort, incremental sort, memoize) have DSM state but require no reinitialization.

The function uses planstate_tree_walker to recursively traverse the entire plan tree, ensuring all descendant nodes are processed.

## Parameters / Member Variables
- `planstate`: The current PlanState node being processed in the plan tree traversal
- `pcxt`: The ParallelContext containing shared memory and coordination information for the parallel query

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [ExecSeqScanReInitializeDSM](ExecSeqScanReInitializeDSM.md)
  - [ExecIndexScanReInitializeDSM](ExecIndexScanReInitializeDSM.md)  
  - [ExecIndexOnlyScanReInitializeDSM](ExecIndexOnlyScanReInitializeDSM.md)
  - [ExecForeignScanReInitializeDSM](ExecForeignScanReInitializeDSM.md)
  - [ExecAppendReInitializeDSM](ExecAppendReInitializeDSM.md)
  - [ExecCustomScanReInitializeDSM](ExecCustomScanReInitializeDSM.md)
  - [ExecBitmapHeapReInitializeDSM](ExecBitmapHeapReInitializeDSM.md)
  - [ExecHashJoinReInitializeDSM](ExecHashJoinReInitializeDSM.md)
  - planstate_tree_walker
- Called from (representative examples):
  - [ExecParallelReinitialize](ExecParallelReinitialize.md)
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md) (recursive calls)

## Notes and Other Information
- This is a static function internal to execParallel.c
- Only processes nodes marked as parallel_aware in their plan structure
- Some node types (HashState, SortState, IncrementalSortState, MemoizeState) have DSM state but explicitly require no reinitialization
- The recursive tree walking ensures comprehensive coverage of all parallel-aware nodes in complex plan trees
- Essential for proper parallel query restart semantics in PostgreSQL

## Simplified Source

```c
static bool
ExecParallelReInitializeDSM(PlanState *planstate, ParallelContext *pcxt)
{
    if (planstate == NULL)
        return false;

    // Call reinitializers for parallel-aware DSM-using plan nodes
    if (planstate->plan->parallel_aware) {
        switch (nodeTag(planstate)) {
            case T_SeqScanState:
                ExecSeqScanReInitializeDSM((SeqScanState *) planstate, pcxt);
                break;
            case T_IndexScanState:
                ExecIndexScanReInitializeDSM((IndexScanState *) planstate, pcxt);
                break;
            case T_IndexOnlyScanState:
                ExecIndexOnlyScanReInitializeDSM((IndexOnlyScanState *) planstate, pcxt);
                break;
            case T_ForeignScanState:
                ExecForeignScanReInitializeDSM((ForeignScanState *) planstate, pcxt);
                break;
            case T_AppendState:
                ExecAppendReInitializeDSM((AppendState *) planstate, pcxt);
                break;
            case T_CustomScanState:
                ExecCustomScanReInitializeDSM((CustomScanState *) planstate, pcxt);
                break;
            case T_BitmapHeapScanState:
                ExecBitmapHeapReInitializeDSM((BitmapHeapScanState *) planstate, pcxt);
                break;
            case T_HashJoinState:
                ExecHashJoinReInitializeDSM((HashJoinState *) planstate, pcxt);
                break;
            case T_HashState:
            case T_SortState:
            case T_IncrementalSortState:
            case T_MemoizeState:
                // These nodes have DSM state but no reinitialization required
                break;
        }
    }

    // Recursively traverse plan tree
    return planstate_tree_walker(planstate, ExecParallelReInitializeDSM, pcxt);
}
```