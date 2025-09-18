# ExecInitPartitionPruning

## Location
src/backend/executor/execPartition.c: 1801 - 1866

## Overview
Initializes the data structure needed for run-time partition pruning and performs initial pruning if needed during executor startup.

## Definition
```c
PartitionPruneState *ExecInitPartitionPruning(PlanState *planstate, int n_total_subplans, PartitionPruneInfo *pruneinfo, Bitmapset **initially_valid_subplans)
```

## Detailed Description
This function creates and initializes a PartitionPruneState structure that enables run-time partition pruning for partitioned table queries. It supports dynamic elimination of irrelevant partitions during execution, particularly when partition key comparisons involve non-constant but stable expressions. The function performs initial pruning if configured to do so, determining which subplans need to be initialized. It also handles re-sequencing of subplan maps when some subplans are eliminated during initial pruning.

## Parameters / Member Variables
- `planstate`: The plan state node (typically Append or MergeAppend) that contains the subplans
- `n_total_subplans`: Total number of subplans in the parent plan node
- `pruneinfo`: Partition pruning information from the planner containing pruning steps and expressions
- `initially_valid_subplans`: Output parameter returning a bitmapset of subplan indexes that must be initialized

## Dependencies
- Functions called/Symbols referenced:
  - ExecAssignExprContext
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [ExecFindMatchingSubPlans](ExecFindMatchingSubPlans.md)
  - [bms_add_range](../b/bms_add_range.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [PartitionPruneFixSubPlanMap](../P/PartitionPruneFixSubPlanMap.md)
- Called from (representative examples):
  - [ExecInitAppend](ExecInitAppend.md)
  - ExecInitMergeAppend

## Notes and Other Information
- Supports both Append and MergeAppend plan types with arbitrary numbers of subplans
- Distinguishes between expressions with PARAM_EXEC parameters (requiring per-scan pruning) and stable expressions (allowing one-time startup pruning)
- When initial pruning eliminates subplans, the function updates internal maps to account for the reduced subplan set
- The initially_valid_subplans output indicates which child subplans must be initialized alongside the parent plan node
- Part of PostgreSQL's run-time partition pruning framework that allows dynamic elimination of irrelevant partitions during query execution