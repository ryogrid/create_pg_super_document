# ReScanExprContext

## Location
[src/backend/executor/execUtils.c:441-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L441-L455)

## Overview
Resets an ExprContext in preparation for rescanning its associated plan node, ensuring proper cleanup of callbacks and memory contexts.

## Definition

```c
void
ReScanExprContext(ExprContext *econtext)
```
## Detailed Description
ReScanExprContext prepares an ExprContext for reuse by performing a controlled reset operation. This function is essential when a plan node needs to be rescanned, as it ensures that any partially completed set-returning functions are properly canceled and that the per-tuple memory context is cleared for fresh allocations.

The function executes all registered shutdown callbacks through ShutdownExprContext (with isCommit=true), then resets the per-tuple memory context rather than deleting it. This approach maintains the context structure while freeing all allocated memory within it, making it ready for the next scan iteration.

Unlike FreeExprContext, this function preserves the ExprContext structure itself and only cleans up transient state, making it suitable for rescan operations where the context will be reused.

## Parameters / Member Variables
- : The ExprContext structure to be reset for rescanning

## Dependencies
- Functions called/Symbols referenced:
  - [ShutdownExprContext](../S/ShutdownExprContext.md) (executes shutdown callbacks with isCommit=true)
  - [MemoryContextReset](../M/MemoryContextReset.md) (clears per-tuple memory context without deleting it)

- Called from (representative examples):
  - [ExecReScan](../E/ExecReScan.md) (in src/backend/executor/execAmi.c:127)
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md) (in src/backend/executor/nodeAgg.c:2248, 2268)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md) (in src/backend/executor/nodeAgg.c:2624)
  - [ExecEndAgg](../E/ExecEndAgg.md) (in src/backend/executor/nodeAgg.c:4355, 4357)
  - [ExecReScanAgg](../E/ExecReScanAgg.md) (in src/backend/executor/nodeAgg.c:4428, 4456)
  - [ValuesNext](../V/ValuesNext.md) (in src/backend/executor/nodeValuesscan.c:101)
  - [domain_check_input](../d/domain_check_input.md) (in src/backend/utils/adt/domains.c:219)

## Notes and Other Information
- This function is crucial for proper rescan behavior in PostgreSQL's executor
- Preserves the ExprContext structure while clearing transient state
- Essential for canceling partially complete set-returning functions during rescans
- Uses MemoryContextReset instead of MemoryContextDelete to maintain the context for reuse
- Always calls shutdown callbacks with isCommit=true since this is a controlled reset operation
- Commonly used in aggregate nodes and other operators that support rescanning
- The function makes no assumptions about the caller's memory context

## Simplified Source

```c
void ReScanExprContext(ExprContext *econtext) {
    // Execute shutdown callbacks to cancel any partial operations
    ShutdownExprContext(econtext, true);

    // Clear per-tuple memory but keep the context structure
    MemoryContextReset(econtext->ecxt_per_tuple_memory);
}
```