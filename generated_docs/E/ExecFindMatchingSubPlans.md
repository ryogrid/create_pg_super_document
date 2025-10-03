# ExecFindMatchingSubPlans

## Location
[src/backend/executor/execPartition.c:2303-2365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L2303-L2365)

## Overview
Determines which subplans match the partition pruning steps for the current comparison expression values, returning a bitmap of valid subplan indexes.

## Definition

```c
Bitmapset *
ExecFindMatchingSubPlans(PartitionPruneState *prunestate,
						 bool initial_prune)
```
## Detailed Description
This function is the main entry point for executor-time partition pruning. It iterates through all partition hierarchies defined in the pruning state and determines which subplans should be executed based on the current runtime parameter values. The function supports two modes of operation:

1. **Initial pruning**: Performed during executor initialization when PARAM_EXEC parameters cannot yet be evaluated
2. **Runtime pruning**: Performed during execution when all parameters are available and can be evaluated

The function uses a temporary memory context to avoid memory leaks in the executor's query-lifespan context. For each partition hierarchy, it delegates the actual pruning work to the recursive helper function . After processing all hierarchies, it adds any additional subplans that weren't handled by partition pruning logic.

## Parameters / Member Variables
- : Partition pruning state containing all pruning information, contexts, and subplan mappings
- : Boolean flag indicating whether this is initial pruning (true) or runtime pruning (false)

## Dependencies
- Functions called/Symbols referenced:
  - [find_matching_subplans_recurse](../f/find_matching_subplans_recurse.md)
  - ResetExprContext 
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_copy](../b/bms_copy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [ExecInitPartitionPruning](ExecInitPartitionPruning.md)
  - [choose_next_subplan_locally](../c/choose_next_subplan_locally.md)
  - [choose_next_subplan_for_leader](../c/choose_next_subplan_for_leader.md)
  - [choose_next_subplan_for_worker](../c/choose_next_subplan_for_worker.md)
  - [ExecAppendAsyncBegin](ExecAppendAsyncBegin.md)
  - [ExecMergeAppend](ExecMergeAppend.md)

## Notes and Other Information
- The function is defined in src/backend/executor/execPartition.c:2303-2365
- Memory management is carefully handled using temporary contexts to prevent memory leaks
- The function works with complex partition hierarchies and handles both simple partitioned tables and multi-level partitioning schemes
- Expression evaluation contexts are reset after use to free temporary memory
- The returned bitmapset must be freed by the caller

## Simplified Source

```c
Bitmapset *
ExecFindMatchingSubPlans(PartitionPruneState *prunestate, bool initial_prune)
{
    Bitmapset *result = NULL;
    MemoryContext oldcontext;

    // Validate we can proceed with pruning
    Assert(initial_prune || prunestate->do_exec_prune);

    // Switch to temporary context to avoid memory leaks
    oldcontext = MemoryContextSwitchTo(prunestate->prune_context);

    // Process each partition hierarchy
    for (int i = 0; i < prunestate->num_partprunedata; i++)
    {
        PartitionPruningData *prunedata = prunestate->partprunedata[i];
        PartitionedRelPruningData *pprune = &prunedata->partrelprunedata[0];

        // Find matching subplans for this hierarchy (recursive)
        find_matching_subplans_recurse(prunedata, pprune, initial_prune, &result);

        // Reset expression context if pruning steps were executed
        if (pprune->exec_pruning_steps)
            ResetExprContext(pprune->exec_context.exprcontext);
    }

    // Add any subplans not handled by partition pruning
    result = bms_add_members(result, prunestate->other_subplans);

    // Copy result out of temp context before cleanup
    MemoryContextSwitchTo(oldcontext);
    result = bms_copy(result);
    MemoryContextReset(prunestate->prune_context);

    return result;
}
```