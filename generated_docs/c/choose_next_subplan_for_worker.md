# choose_next_subplan_for_worker

## Location
[src/backend/executor/nodeAppend.c:702-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L702-L827)

## Overview
Chooses the next subplan for a parallel-aware Append node to execute, coordinating work distribution among parallel workers by managing the selection and assignment of subplans in a thread-safe manner.

## Definition

```c
static bool
choose_next_subplan_for_worker(AppendState *node)
```
## Detailed Description
This function implements the core work distribution logic for parallel-aware Append nodes in PostgreSQL's executor. It operates under exclusive locking to ensure thread-safe coordination among multiple parallel workers. The function follows a specific strategy for subplan assignment:

1. **Non-partial plans first**: Assigns non-partial plans in order of descending cost, with each plan executed by a single worker
2. **Partial plan distribution**: After non-partial plans are exhausted, distributes partial plans evenly across available workers
3. **Cyclic assignment**: When reaching the end of valid subplans, loops back to the first partial plan to ensure even work distribution

The function handles runtime partition pruning by identifying valid subplans on the first call and marking invalid subplans as finished. It maintains state through the ParallelAppendState structure, tracking which subplans are completed and determining the next available subplan for execution.

## Parameters / Member Variables
- `*node`: Pointer to AppendState containing the append node's execution state, parallel state information, and subplan tracking data
## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward (direction validation)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (thread synchronization)
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md) (runtime pruning support)
  - [mark_invalid_subplans_as_finished](../m/mark_invalid_subplans_as_finished.md) (pruning cleanup)
  - [bms_next_member](../b/bms_next_member.md) (bitmap set iteration)
- Called from (representative examples):
  - [ExecAppendInitializeWorker](../E/ExecAppendInitializeWorker.md) (worker initialization)

## Notes and Other Information
- Only supports forward scans (backward scans are not supported in parallel-aware plans)
- Uses exclusive locking on pa_lock to coordinate between parallel workers
- Immediately marks non-partial plans as finished since they cannot be shared between workers
- Returns false when no more subplans are available for execution
- Critical for load balancing in parallel query execution
- Part of PostgreSQL's parallel query execution infrastructure introduced for improved performance on multi-core systems

## Simplified Source

```c
static bool choose_next_subplan_for_worker(AppendState *node) {
    ParallelAppendState *pstate = node->as_pstate;

    // Only support forward scans in parallel execution
    Assert(ScanDirectionIsForward(node->ps.state->es_direction));
    Assert(node->as_nplans > 0);

    // Acquire exclusive lock for thread-safe coordination
    LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE);

    // Mark current subplan as finished if valid
    if (node->as_whichplan != INVALID_SUBPLAN_INDEX) {
        node->as_pstate->pa_finished[node->as_whichplan] = true;
    }
    // Initialize valid subplans on first call (handles runtime pruning)
    else if (!node->as_valid_subplans_identified) {
        node->as_valid_subplans = ExecFindMatchingSubPlans(node->as_prune_state, false);
        node->as_valid_subplans_identified = true;
        mark_invalid_subplans_as_finished(node);
    }

    // Check if all plans are completed
    if (pstate->pa_next_plan == INVALID_SUBPLAN_INDEX) {
        LWLockRelease(&pstate->pa_lock);
        return false;
    }

    // Find next available subplan using round-robin strategy
    node->as_whichplan = pstate->pa_next_plan;
    while (pstate->pa_finished[pstate->pa_next_plan]) {
        int nextplan = bms_next_member(node->as_valid_subplans, pstate->pa_next_plan);

        if (nextplan >= 0) {
            pstate->pa_next_plan = nextplan;
        }
        // Loop back to partial plans when non-partial plans exhausted
        else if (node->as_whichplan > node->as_first_partial_plan) {
            nextplan = bms_next_member(node->as_valid_subplans,
                                       node->as_first_partial_plan - 1);
            pstate->pa_next_plan = nextplan < 0 ? node->as_whichplan : nextplan;
        }
        else {
            pstate->pa_next_plan = node->as_whichplan;
        }

        // No more plans available
        if (pstate->pa_next_plan == node->as_whichplan) {
            pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
            LWLockRelease(&pstate->pa_lock);
            return false;
        }
    }

    // Select chosen plan and advance to next
    node->as_whichplan = pstate->pa_next_plan;
    pstate->pa_next_plan = bms_next_member(node->as_valid_subplans, pstate->pa_next_plan);

    // Handle wrap-around to partial plans
    if (pstate->pa_next_plan < 0) {
        int nextplan = bms_next_member(node->as_valid_subplans,
                                       node->as_first_partial_plan - 1);
        pstate->pa_next_plan = nextplan >= 0 ? nextplan : INVALID_SUBPLAN_INDEX;
    }

    // Non-partial plans are immediately marked finished (not shared)
    if (node->as_whichplan < node->as_first_partial_plan) {
        node->as_pstate->pa_finished[node->as_whichplan] = true;
    }

    LWLockRelease(&pstate->pa_lock);
    return true;
}
```