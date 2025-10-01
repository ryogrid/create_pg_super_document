# get_matching_partitions

## Location
[src/backend/partitioning/partprune.c:817-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L817-L960)

## Overview
Determines which partitions survive partition pruning by executing a list of pruning steps and returning a bitmapset of the surviving partition indexes.

## Definition

```c
Bitmapset *
get_matching_partitions(PartitionPruneContext *context, List *pruning_steps)
```
## Detailed Description
This function is the main entry point for partition pruning execution. It processes a list of pruning steps in sequence, where each step can be either a base pruning operation (PartitionPruneStepOp) or a combination operation (PartitionPruneStepCombine). The function allocates space for storing intermediate results from each pruning step, then iterates through all steps, executing them based on their type.

After all pruning steps are executed, the function collects the final result which contains bound offsets of datums whose corresponding partitions should be included. It then translates these bound offsets into actual partition indexes, handling special cases like null-accepting partitions and default partitions.

The function supports all PostgreSQL partitioning strategies (LIST, RANGE, HASH) and properly handles edge cases where bounds don't correspond to actual partitions, marking the default partition for scanning when appropriate.

## Parameters / Member Variables
- : PartitionPruneContext containing partition metadata, bound information, strategy, and execution context
- : List of PartitionPruneStep objects to be executed in sequence

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_range](../b/bms_add_range.md)
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md)
  - [perform_pruning_combine_step](../p/perform_pruning_combine_step.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - partition_bound_has_default
  - partition_bound_accepts_nulls
  - nodeTag
- Called from (representative examples):
  - [find_matching_subplans_recurse](../f/find_matching_subplans_recurse.md) (execPartition.c:2383, 2386)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md) (partprune.c:803)

## Notes and Other Information
- Returns all partitions if no pruning steps are provided
- Requires context->exprcontext to be valid when pruning_steps were generated with targets other than PARTTARGET_PLANNER
- Handles special partition types: null-accepting partitions (LIST strategy only) and default partitions (LIST/RANGE strategies)
- The function performs bounds checking and validates partition indexes before adding them to the result set
- Memory allocation for results array uses palloc0 to ensure proper initialization

## Simplified Source

```c
Bitmapset *
get_matching_partitions(PartitionPruneContext *context, List *pruning_steps)
{
    Bitmapset *result;
    int num_steps = list_length(pruning_steps);
    PruneStepResult **results, *final_result;
    ListCell *lc;
    bool scan_default;

    // Return all partitions if no pruning steps provided
    if (num_steps == 0) {
        return bms_add_range(NULL, 0, context->nparts - 1);
    }

    // Allocate space for storing intermediate step results
    results = (PruneStepResult **) palloc0(num_steps * sizeof(PruneStepResult *));

    // Execute each pruning step in sequence
    foreach(lc, pruning_steps) {
        PartitionPruneStep *step = lfirst(lc);

        switch (nodeTag(step)) {
            case T_PartitionPruneStepOp:
                // Perform base pruning operation
                results[step->step_id] = perform_pruning_base_step(context,
                                                                  (PartitionPruneStepOp *) step);
                break;

            case T_PartitionPruneStepCombine:
                // Combine results from previous steps
                results[step->step_id] = perform_pruning_combine_step(context,
                                                                     (PartitionPruneStepCombine *) step,
                                                                     results);
                break;

            default:
                elog(ERROR, "invalid pruning step type: %d", (int) nodeTag(step));
        }
    }

    // Get final pruning result and convert bound offsets to partition indexes
    final_result = results[num_steps - 1];
    result = NULL;
    scan_default = final_result->scan_default;

    int i = -1;
    while ((i = bms_next_member(final_result->bound_offsets, i)) >= 0) {
        int partindex = context->boundinfo->indexes[i];

        if (partindex < 0) {
            // No partition covers this range, check if we need default partition
            scan_default |= partition_bound_has_default(context->boundinfo);
            continue;
        }

        result = bms_add_member(result, partindex);
    }

    // Add special partitions if needed
    if (final_result->scan_null) {
        result = bms_add_member(result, context->boundinfo->null_index);
    }
    if (scan_default) {
        result = bms_add_member(result, context->boundinfo->default_index);
    }

    return result;
}
```