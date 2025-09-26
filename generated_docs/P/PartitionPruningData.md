# PartitionPruningData

## Location
src/include/executor/execPartition.h: 78 - 82

## Overview
PartitionPruningData holds all the run-time pruning information for a single partitioning hierarchy containing one or more partitions, organizing the data in a parent-before-children array structure.

## Definition

```c
typedef struct PartitionPruningData
{
	int			num_partrelprunedata;	/* number of array entries */
	PartitionedRelPruningData partrelprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruningData;
```
## Detailed Description
This structure serves as the top-level container for partition pruning information in PostgreSQL's executor. It manages a complete partitioning hierarchy by storing an array of PartitionedRelPruningData structures, where each entry represents one level of partitioning. The array is carefully ordered such that parent partitions appear before their children, with the first entry always being the topmost partition that was explicitly named in the SQL query. This hierarchical organization enables efficient traversal and pruning decisions during query execution.

## Parameters / Member Variables
- : Number of entries in the partrelprunedata array, representing the total count of partitioned relations in the hierarchy
- : Flexible array of PartitionedRelPruningData structures, ordered with parents before children, containing pruning information for each partitioned relation in the hierarchy

## Dependencies
- Functions called/Symbols referenced:
  - PartitionedRelPruningData (component structure)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array)
- Called from (representative examples):
  - CreatePartitionPruneState
  - ExecFindMatchingSubPlans
  - PartitionPruneFixSubPlanMap
  - find_matching_subplans_recurse

## Notes and Other Information
- The ordering constraint (parents before children) is critical for proper hierarchy traversal during pruning
- The first array entry is always the topmost partition explicitly named in the SQL query
- Uses flexible array member pattern for efficient memory allocation of variable-sized hierarchies
- This structure is typically created during executor initialization and used throughout query execution
- Works in conjunction with PartitionPruneState to maintain execution state for partition pruning operations
- Essential for multi-level partitioned table performance optimization by enabling systematic pruning decisions