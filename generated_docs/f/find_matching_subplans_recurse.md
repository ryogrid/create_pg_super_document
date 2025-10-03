# find_matching_subplans_recurse

## Location
[src/backend/executor/execPartition.c:2366-2418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L2366-L2418)

## Overview
A recursive worker function for ExecFindMatchingSubPlans that processes partition hierarchies to identify valid (non-prunable) subplans.

## Definition

```c
static void
find_matching_subplans_recurse(PartitionPruningData *prunedata,
							   PartitionedRelPruningData *pprune,
							   bool initial_prune,
							   Bitmapset **validsubplans)
```
## Detailed Description
This function is the core recursive engine that traverses partition hierarchies to determine which subplans should be executed. It operates by first determining which partitions should be included based on the current pruning context, then translating those partitions into subplan indexes or recursively processing sub-partitions.

The function handles two execution modes:
1. **Initial pruning**: Uses initial_pruning_steps when PARAM_EXEC parameters are not yet available
2. **Runtime pruning**: Uses exec_pruning_steps when all parameters can be evaluated

For each partition found to be valid, the function either adds the corresponding subplan index to the result set or recursively processes sub-partitions if the partition has been further partitioned. The function includes protection against stack overflow due to deeply nested partition hierarchies.

## Parameters / Member Variables
- : Overall partition pruning data structure containing all partitioning information
- : Specific partitioned relation pruning data for the current level in the hierarchy
- : Boolean indicating whether this is initial pruning (true) or runtime pruning (false)
- : Output parameter - pointer to bitmapset that accumulates valid subplan indexes

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [get_matching_partitions](../g/get_matching_partitions.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [find_matching_subplans_recurse](find_matching_subplans_recurse.md) (recursive call)
- Called from (representative examples):
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md)
  - [find_matching_subplans_recurse](find_matching_subplans_recurse.md) (recursive calls)

## Notes and Other Information
- The function is defined in src/backend/executor/execPartition.c:2366-2418
- Includes stack depth checking to prevent stack overflow in deeply nested partition hierarchies
- Handles cases where the planner has already pruned all sub-partitions for a partition
- The function modifies the validsubplans bitmapset in-place, accumulating results across recursive calls
- Static function scope indicates it's an internal implementation detail of the partition pruning system
- Properly handles partition-to-subplan mapping through the subplan_map and subpart_map arrays

## Simplified Source

```c
static void
find_matching_subplans_recurse(PartitionPruningData *prunedata,
                               PartitionedRelPruningData *pprune,
                               bool initial_prune,
                               Bitmapset **validsubplans)
{
    Bitmapset *partset;
    int i;

    // Guard against stack overflow in deep partition hierarchies
    check_stack_depth();

    // Determine which partitions to include based on pruning context
    if (initial_prune && pprune->initial_pruning_steps) {
        // Use initial pruning steps when parameters not yet available
        partset = get_matching_partitions(&pprune->initial_context,
                                          pprune->initial_pruning_steps);
    } else if (!initial_prune && pprune->exec_pruning_steps) {
        // Use runtime pruning steps when all parameters available
        partset = get_matching_partitions(&pprune->exec_context,
                                          pprune->exec_pruning_steps);
    } else {
        // No pruning steps available, include all present partitions
        partset = pprune->present_parts;
    }

    // Process each partition in the result set
    i = -1;
    while ((i = bms_next_member(partset, i)) >= 0) {
        if (pprune->subplan_map[i] >= 0) {
            // Direct mapping to subplan - add to result set
            *validsubplans = bms_add_member(*validsubplans,
                                            pprune->subplan_map[i]);
        } else {
            // This partition has sub-partitions
            int partidx = pprune->subpart_map[i];

            if (partidx >= 0) {
                // Recursively process sub-partitions
                find_matching_subplans_recurse(prunedata,
                                               &prunedata->partrelprunedata[partidx],
                                               initial_prune, validsubplans);
            }
            // If partidx < 0, planner already pruned all sub-partitions
            // Silently ignore this case
        }
    }
}
```