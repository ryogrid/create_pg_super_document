# freestate_cluster

## Location
[src/backend/utils/sort/tuplesortvariants.c:1399-1422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1399-L1422)

## Overview
Cleans up and releases execution state resources specifically allocated for CLUSTER sort operations, including expression contexts and tuple table slots.

## Definition

```c
static void
freestate_cluster(Tuplesortstate *state)
```
## Detailed Description
The `freestate_cluster` function is responsible for properly cleaning up execution state resources that were allocated specifically for CLUSTER operations. During CLUSTER sort operations, PostgreSQL may create an executor state (EState) to handle expression evaluation and tuple processing. This function ensures that all such resources are properly released to prevent memory leaks.

The cleanup process involves:
1. Retrieving the executor state from the cluster-specific arguments
2. Obtaining the per-tuple expression context from the executor state
3. Dropping the scan tuple slot that was allocated for tuple processing
4. Freeing the entire executor state and its associated resources

This function is called as part of the tuplesort cleanup process to ensure that all CLUSTER-specific resources are properly deallocated.

## Parameters / Member Variables
- `state`: The tuplesort state containing the CLUSTER-specific execution context to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - GetPerTupleExprContext
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
- Called from (representative examples):
  - CLUSTER_SORT operations cleanup
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md) cleanup phase

## Notes and Other Information
- This function only performs cleanup if an executor state was actually created (arg->estate != NULL check)
- The function specifically handles the cleanup of expression contexts and tuple table slots that are unique to CLUSTER operations
- Proper cleanup is essential to prevent memory leaks during large CLUSTER operations
- The function is part of the tuplesort variant system that provides operation-specific resource management
- Works in conjunction with the previously processed symbols `ExecDropSingleTupleTableSlot` and `FreeExecutorState` to ensure complete cleanup

## Simplified Source

```c
static void
freestate_cluster(Tuplesortstate *state)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortClusterArg *arg = (TuplesortClusterArg *) base->arg;

    // Clean up executor state if it was created for CLUSTER operations
    if (arg->estate != NULL) {
        ExprContext *econtext = GetPerTupleExprContext(arg->estate);

        // Drop the scan tuple slot and free executor state
        ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
        FreeExecutorState(arg->estate);
    }
}
```