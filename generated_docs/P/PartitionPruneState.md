# PartitionPruneState

## Location
src/include/executor/execPartition.h: 113 - 122

## Overview
PartitionPruneState is a state object that enables plan nodes to perform run-time partition pruning by eliminating subplans that cannot produce matching tuples based on query clauses.

## Definition

```c
typedef struct PartitionPruneState
{
	Bitmapset  *execparamids;
	Bitmapset  *other_subplans;
	MemoryContext prune_context;
	bool		do_initial_prune;
	bool		do_exec_prune;
	int			num_partprunedata;
	PartitionPruningData *partprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruneState;
```
## Detailed Description
This structure is the central execution state object for PostgreSQL's run-time partition pruning mechanism. It can be attached to plan types that support arbitrary lists of subplans containing partitions (such as Append and MergeAppend nodes) to eliminate subplans whose partitions cannot possibly produce tuples matching the query conditions. The structure tracks execution parameters that affect pruning decisions, maintains a dedicated memory context for pruning operations, and holds references to the pruning data for all partitioned relations involved in the query.

## Parameters / Member Variables
- : Bitmapset containing parameter IDs of PARAM_EXEC parameters found within partprunedata structs; pruning must be redone when any of these parameter values change
- : Bitmapset with indexes of subplans that don't belong to any partprunedata (e.g., UNION ALL children that aren't partitioned tables, or partitioned tables deemed unsuitable for run-time pruning); these must not be pruned
- : Short-lived memory context used for executing partition pruning functions to avoid memory leaks
- : Boolean flag indicating whether pruning should be performed during executor startup (at any hierarchy level)
- : Boolean flag indicating whether pruning should be performed during executor runtime (at any hierarchy level)  
- : Number of items in the partprunedata array
- : Flexible array of PartitionPruningData pointers, one for each partitioning hierarchy that requires run-time pruning

## Dependencies
- Functions called/Symbols referenced:
  - PartitionPruningData (referenced partition pruning data)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array)
- Called from (representative examples):
  - ExecInitPartitionPruning
  - CreatePartitionPruneState
  - ExecFindMatchingSubPlans
  - PartitionPruneFixSubPlanMap
  - ExecInitAppend
  - ExecInitMergeAppend

## Notes and Other Information
- Primarily used by Append and MergeAppend plan nodes but can be attached to any plan type supporting subplan lists
- The execparamids tracking ensures pruning is re-evaluated whenever relevant parameters change, critical for prepared statements and parameterized queries
- Memory context management prevents memory leaks during repeated pruning operations
- Supports both one-time startup pruning and repeated runtime pruning for optimal performance
- The other_subplans mechanism ensures non-partitioned subplans are never incorrectly eliminated
- Essential for achieving good performance with partitioned tables by reducing the number of partitions that need to be scanned