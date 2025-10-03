# ExecInitPartitionPruning

## Location
[src/backend/executor/execPartition.c:1801-1866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1801-L1866)

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
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md)
  - [ExecFindMatchingSubPlans](ExecFindMatchingSubPlans.md)
  - [bms_add_range](../b/bms_add_range.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [PartitionPruneFixSubPlanMap](../P/PartitionPruneFixSubPlanMap.md)
- Called from (representative examples):
  - [ExecInitAppend](ExecInitAppend.md)
  - [ExecInitMergeAppend](ExecInitMergeAppend.md)

## Notes and Other Information
- Supports both Append and MergeAppend plan types with arbitrary numbers of subplans
- Distinguishes between expressions with PARAM_EXEC parameters (requiring per-scan pruning) and stable expressions (allowing one-time startup pruning)
- When initial pruning eliminates subplans, the function updates internal maps to account for the reduced subplan set
- The initially_valid_subplans output indicates which child subplans must be initialized alongside the parent plan node
- Part of PostgreSQL's run-time partition pruning framework that allows dynamic elimination of irrelevant partitions during query execution

## Simplified Source

```c
PartitionPruneState *ExecInitPartitionPruning(PlanState *planstate,
                                              int n_total_subplans,
                                              PartitionPruneInfo *pruneinfo,
                                              Bitmapset **initially_valid_subplans) {
    PartitionPruneState *prunestate;
    EState *estate = planstate->state;

    // Set up expression context for partition expressions
    ExecAssignExprContext(estate, planstate);

    // Create pruning state structure
    prunestate = CreatePartitionPruneState(planstate, pruneinfo);

    // Perform initial pruning if configured
    if (prunestate->do_initial_prune) {
        *initially_valid_subplans = ExecFindMatchingSubPlans(prunestate, true);
    } else {
        // No pruning - initialize all subplans
        *initially_valid_subplans = bms_add_range(NULL, 0, n_total_subplans - 1);
    }

    // Update subplan maps if some were pruned away
    if (bms_num_members(*initially_valid_subplans) < n_total_subplans) {
        if (prunestate->do_exec_prune) {
            PartitionPruneFixSubPlanMap(prunestate, *initially_valid_subplans, n_total_subplans);
        }
    }

    return prunestate;
}
```