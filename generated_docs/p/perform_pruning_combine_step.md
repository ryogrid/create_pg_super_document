# perform_pruning_combine_step

## Location
[src/backend/partitioning/partprune.c:3564-3672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3564-L3672)

## Overview
Combines the results of multiple partition pruning steps using union or intersection operations to determine the final set of partition indexes that need to be scanned.

## Definition


## Detailed Description
This function processes a PartitionPruneStepCombine node to combine the results from multiple source pruning steps. It supports two combination operations:

1. **UNION (PARTPRUNE_COMBINE_UNION)**: Merges all partition indexes from source steps, including any partitions that satisfy at least one of the conditions.

2. **INTERSECT (PARTPRUNE_COMBINE_INTERSECT)**: Finds only the partition indexes that are common to all source steps, representing partitions that satisfy all conditions simultaneously.

The function handles special cases like steps with no source IDs (indicating no pruning should be performed) and properly manages flags for scanning null and default partitions based on the combination operation.

## Parameters / Member Variables
- : PartitionPruneContext containing partitioning metadata and bound information
- : PartitionPruneStepCombine node specifying the combination operation and source step IDs
- : Array of PruneStepResult pointers from previously executed pruning steps

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