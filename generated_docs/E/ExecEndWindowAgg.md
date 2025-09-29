# ExecEndWindowAgg

## Location
[src/backend/executor/nodeWindowAgg.c:2681-2707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2681-L2707)

## Overview
ExecEndWindowAgg is the cleanup function for the WindowAgg executor node that releases all allocated resources and terminates the execution state.

## Definition
```c
void ExecEndWindowAgg(WindowAggState *node)
```

## Detailed Description
ExecEndWindowAgg performs comprehensive cleanup for a WindowAgg executor node when execution is complete. The function systematically releases all memory contexts and data structures that were allocated during the window aggregate execution lifecycle. It first releases partition-specific resources through release_partition(), then deallocates per-aggregate memory contexts that differ from the main aggregate context, and finally cleans up the main memory contexts (partcontext and aggcontext). The function also frees the perfunc and peragg arrays and recursively terminates the outer plan node.

## Parameters / Member Variables
- `node`: Pointer to the WindowAggState structure containing the execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [release_partition](../r/release_partition.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pfree](../p/pfree.md)
  - outerPlanState
  - [ExecEndNode](ExecEndNode.md)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (general executor cleanup)

## Notes and Other Information
- This function is part of the standard PostgreSQL executor node cleanup protocol
- The function carefully handles multiple memory contexts to prevent memory leaks
- Per-aggregate contexts are only deleted if they differ from the main aggregate context
- The cleanup order is important: partition resources first, then per-aggregate contexts, then main contexts
- Located in src/backend/executor/nodeWindowAgg.c:2681-2707

## Simplified Source

```c
void ExecEndWindowAgg(WindowAggState *node) {
    // Release partition-specific resources
    release_partition(node);

    // Clean up per-aggregate memory contexts that differ from main context
    for (int i = 0; i < node->numaggs; i++) {
        if (node->peragg[i].aggcontext != node->aggcontext)
            MemoryContextDelete(node->peragg[i].aggcontext);
    }

    // Delete main memory contexts
    MemoryContextDelete(node->partcontext);
    MemoryContextDelete(node->aggcontext);

    // Free arrays
    pfree(node->perfunc);
    pfree(node->peragg);

    // Shut down the outer plan
    ExecEndNode(outerPlanState(node));
}
```