# PartitionedRelPruningData

## Location
[src/include/executor/execPartition.h:59-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execPartition.h#L59-L69)

## Overview
PartitionedRelPruningData contains per-partitioned-table data for run-time pruning of partitions, with one instance for the topmost partition plus one for each non-leaf child partition in multilevel partitioned tables.

## Definition

```c
typedef struct PartitionedRelPruningData
{
	int			nparts;
	int		   *subplan_map;
	int		   *subpart_map;
	Bitmapset  *present_parts;
	List	   *initial_pruning_steps;
	List	   *exec_pruning_steps;
	PartitionPruneContext initial_context;
	PartitionPruneContext exec_context;
} PartitionedRelPruningData;
```
## Detailed Description
This structure is essential for PostgreSQL's partition pruning optimization, storing all the information needed to eliminate unnecessary partition scans at execution time. It contains mapping arrays that relate partition indexes to subplan/subpart indexes, bitmapsets tracking which partitions are present, and separate pruning step lists for startup pruning (done once) and per-scan pruning (done for each scan). The structure supports multilevel partitioned tables by maintaining hierarchical relationships through subpart_map indexing into the broader PartitionPruningData array.

## Parameters / Member Variables
- : Length of subplan_map[] and subpart_map[] arrays, representing the total number of partitions
- : Array mapping partition indexes to subplan indexes, with -1 indicating no corresponding subplan
- : Array mapping partition indexes to subpart indexes in PartitionPruningData.partrelprunedata[], with -1 indicating no subpart
- : Bitmapset containing partition indexes that have either subplans or subparts available
- : List of PartitionPruneSteps executed during executor startup for initial pruning
- : List of PartitionPruneSteps executed during each scan for dynamic pruning
- : Execution context details for initial_pruning_steps (only valid if initial_pruning_steps isn't NIL)
- : Execution context details for exec_pruning_steps (only valid if exec_pruning_steps isn't NIL)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionPruneContext](PartitionPruneContext.md) (for execution contexts)
- Called from (representative examples):
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md)
  - [PartitionPruneFixSubPlanMap](PartitionPruneFixSubPlanMap.md)
  - [find_matching_subplans_recurse](../f/find_matching_subplans_recurse.md)

## Notes and Other Information
- The structure maintains the same subplan_map and subpart_map semantics as PartitionedRelPruneInfo from the planner
- Supports both startup pruning (done once per query execution) and per-scan pruning (done for each table scan)
- Critical for performance in partitioned table queries by eliminating scans of irrelevant partitions
- Works in conjunction with PartitionPruningData to handle multilevel partition hierarchies
- The bitmapset optimization allows quick checks of partition availability without scanning arrays