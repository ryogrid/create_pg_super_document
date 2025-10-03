# perform_pruning_combine_step

## Location
[src/backend/partitioning/partprune.c:3564-3672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3564-L3672)

## Overview
Combines the results of multiple partition pruning steps using union or intersection operations to determine the final set of partition indexes that need to be scanned.

## Definition

```c
static PruneStepResult *
perform_pruning_combine_step(PartitionPruneContext *context,
							 PartitionPruneStepCombine *cstep,
							 PruneStepResult **step_results)
```
## Detailed Description
This function processes a PartitionPruneStepCombine node to combine the results from multiple source pruning steps. It supports two combination operations:

1. **UNION (PARTPRUNE_COMBINE_UNION)**: Merges all partition indexes from source steps, including any partitions that satisfy at least one of the conditions.

2. **INTERSECT (PARTPRUNE_COMBINE_INTERSECT)**: Finds only the partition indexes that are common to all source steps, representing partitions that satisfy all conditions simultaneously.

The function handles special cases like steps with no source IDs (indicating no pruning should be performed) and properly manages flags for scanning null and default partitions based on the combination operation.

## Parameters / Member Variables
- `*context`: PartitionPruneContext containing partitioning metadata and bound information
- `*cstep`: PartitionPruneStepCombine node specifying the combination operation and source step IDs
- `**step_results`: Array of PruneStepResult pointers from previously executed pruning steps
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](palloc0.md)
  - [bms_add_range](../b/bms_add_range.md)
  - partition_bound_has_default
  - partition_bound_accepts_nulls
  - lfirst_int
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_int_members](../b/bms_int_members.md)
  - elog
- Called from (representative examples):
  - [get_matching_partitions](../g/get_matching_partitions.md)

## Notes and Other Information
- Returns all partition indexes when source_stepids is NIL (empty), indicating no pruning constraints
- Validates that source step IDs are less than the current step ID to ensure proper execution order
- For UNION operations, progressively adds partition indexes and sets scan flags to true if any step requires scanning null/default partitions
- For INTERSECT operations, starts with the first step's results and progressively narrows down to common partition indexes, setting scan flags to false if any step doesn't require scanning null/default partitions
- Located in src/backend/partitioning/partprune.c:3564-3672

## Simplified Source

```c
static PruneStepResult *perform_pruning_combine_step(PartitionPruneContext *context,
                                                     PartitionPruneStepCombine *cstep,
                                                     PruneStepResult **step_results) {
    PruneStepResult *result = palloc0(sizeof(PruneStepResult));

    // No source steps means no pruning - return all partitions
    if (cstep->source_stepids == NIL) {
        PartitionBoundInfo boundinfo = context->boundinfo;
        result->bound_offsets = bms_add_range(NULL, 0, boundinfo->nindexes - 1);
        result->scan_default = partition_bound_has_default(boundinfo);
        result->scan_null = partition_bound_accepts_nulls(boundinfo);
        return result;
    }

    switch (cstep->combineOp) {
        case PARTPRUNE_COMBINE_UNION:
            // Union: combine all matching partitions from source steps
            foreach(lc1, cstep->source_stepids) {
                int step_id = lfirst_int(lc1);
                PruneStepResult *step_result = step_results[step_id];

                // Add partition indexes from this step
                result->bound_offsets = bms_add_members(result->bound_offsets,
                                                      step_result->bound_offsets);

                // Set scan flags if any step requires scanning
                if (!result->scan_null)
                    result->scan_null = step_result->scan_null;
                if (!result->scan_default)
                    result->scan_default = step_result->scan_default;
            }
            break;

        case PARTPRUNE_COMBINE_INTERSECT:
            // Intersect: find partitions common to all source steps
            bool firststep = true;
            foreach(lc1, cstep->source_stepids) {
                int step_id = lfirst_int(lc1);
                PruneStepResult *step_result = step_results[step_id];

                if (firststep) {
                    // Copy first step's results
                    result->bound_offsets = bms_copy(step_result->bound_offsets);
                    result->scan_null = step_result->scan_null;
                    result->scan_default = step_result->scan_default;
                    firststep = false;
                } else {
                    // Intersect with subsequent steps
                    result->bound_offsets = bms_int_members(result->bound_offsets,
                                                          step_result->bound_offsets);

                    // Clear scan flags if any step doesn't require scanning
                    if (result->scan_null)
                        result->scan_null = step_result->scan_null;
                    if (result->scan_default)
                        result->scan_default = step_result->scan_default;
                }
            }
            break;
    }

    return result;
}
```