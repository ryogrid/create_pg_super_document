# choose_next_subplan_for_leader

## Location
[src/backend/executor/nodeAppend.c:620-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L620-L701)

## Overview
Implements leader process subplan selection strategy for parallel Append execution, optimizing work distribution by preferring cheaper subplans and coordinating with worker processes through shared memory.

## Definition
```c
static bool choose_next_subplan_for_leader(AppendState *node)
```

## Detailed Description
This function implements the subplan selection logic for the leader process in parallel Append execution. It uses a strategy designed to maximize worker utilization by having the leader prefer cheaper subplans (which are ordered last in the plan list), leaving more expensive work for worker processes. The function manages shared state through lightweight locks to coordinate with workers, marks completed subplans as finished, and handles runtime partition pruning by identifying valid subplans and marking invalid ones as finished.

The leader starts from the last (cheapest) subplan and works backward, immediately marking non-partial plans as finished since they can't be shared among multiple processes. This ensures efficient work distribution in parallel execution scenarios.

## Parameters
- `node`: Pointer to the AppendState structure containing execution state and shared memory references

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md)
  - [mark_invalid_subplans_as_finished](../m/mark_invalid_subplans_as_finished.md)
  - INVALID_SUBPLAN_INDEX (constant)
- Called from (representative examples):
  - [ExecAppendInitializeDSM](../E/ExecAppendInitializeDSM.md) (sets as function pointer)

## Notes and Other Information
- Returns `true` if a subplan was selected for execution, `false` if no more subplans are available
- Only supports forward scan direction (backward scans are not supported in parallel execution)
- Uses exclusive locking on `pa_lock` to ensure atomic updates to shared state
- Starts with the last subplan (index `as_nplans - 1`) to prioritize cheaper plans for leader execution
- Marks non-partial plans (`< as_first_partial_plan`) as immediately finished since they cannot be parallelized
- Integrates with runtime partition pruning by calling `ExecFindMatchingSubPlans` and marking invalid subplans as finished
- Sets `pa_next_plan` to `INVALID_SUBPLAN_INDEX` when no more work is available to signal completion to workers
- The function is static and used as a function pointer set during DSM initialization

## Simplified Source

```c
static bool choose_next_subplan_for_leader(AppendState *node) {
    ParallelAppendState *pstate = node->as_pstate;

    // Only forward scans supported in parallel execution
    Assert(ScanDirectionIsForward(node->ps.state->es_direction));
    Assert(node->as_nplans > 0);

    LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE);

    if (node->as_whichplan != INVALID_SUBPLAN_INDEX) {
        // Mark completed subplan as finished
        node->as_pstate->pa_finished[node->as_whichplan] = true;
    } else {
        // First time: start with last (cheapest) subplan
        node->as_whichplan = node->as_nplans - 1;

        // Handle runtime partition pruning if needed
        if (!node->as_valid_subplans_identified) {
            node->as_valid_subplans = ExecFindMatchingSubPlans(node->as_prune_state, false);
            node->as_valid_subplans_identified = true;
            mark_invalid_subplans_as_finished(node);
        }
    }

    // Find next available subplan, working backward from cheapest
    while (pstate->pa_finished[node->as_whichplan]) {
        if (node->as_whichplan == 0) {
            // No more work available
            pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
            node->as_whichplan = INVALID_SUBPLAN_INDEX;
            LWLockRelease(&pstate->pa_lock);
            return false;
        }
        node->as_whichplan--;
    }

    // Mark non-partial plans as immediately finished (can't be shared)
    if (node->as_whichplan < node->as_first_partial_plan) {
        node->as_pstate->pa_finished[node->as_whichplan] = true;
    }

    LWLockRelease(&pstate->pa_lock);
    return true;
}
```