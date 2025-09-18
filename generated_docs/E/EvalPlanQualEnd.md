# EvalPlanQualEnd

## Location
[src/backend/executor/execMain.c:2986-3039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2986-L3039)

## Overview
EvalPlanQualEnd shuts down EPQ execution by cleaning up the EState and associated resources while preserving shared resources from the parent query.

## Definition
```c
void EvalPlanQualEnd(EPQState *epqstate)
```

## Detailed Description
EvalPlanQualEnd is a cut-down version of ExecutorEnd() that performs cleanup when terminating a parent plan state node or when finished with the current EPQ child. Unlike ExecutorEnd(), this function does not close result relations that are shared from the outer query, but it does close any result and trigger target relations that were opened specifically for the EPQ execution.

The function performs the following cleanup operations:
1. Resets and clears the tuple table if it exists
2. Ends execution of all plan state nodes (main plan and subplans)
3. Resets the per-estate tuple table
4. Closes any result and trigger target relations attached to the EPQ EState
5. Frees the executor state completely
6. Marks the EPQState as idle by clearing all active pointers

The function handles cases where EPQ wasn't actually started but tuple tables may still exist, allowing for cleanup after EvalPlanQualSlot() usage without EvalPlanQualBegin().

## Parameters / Member Variables
- `epqstate`: Pointer to EPQState structure containing the EPQ execution context to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - [ExecResetTupleTable](ExecResetTupleTable.md)
  - [ExecEndNode](ExecEndNode.md)
  - [ExecCloseResultRelations](ExecCloseResultRelations.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)

- Called from (representative examples):
  - [EvalPlanQualSetPlan](EvalPlanQualSetPlan.md)
  - [ExecLockRows](ExecLockRows.md)
  - [ExecEndLockRows](ExecEndLockRows.md)
  - [ExecEndModifyTable](ExecEndModifyTable.md)
  - [apply_handle_update_internal](../a/apply_handle_update_internal.md)
  - [apply_handle_delete_internal](../a/apply_handle_delete_internal.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)
  - EvalPlanQualSetSlot

## Notes and Other Information
- This is a public function (not static) and can be called from various executor nodes
- The function is designed to be safe to call even if EPQ execution was never started
- Memory context switching ensures proper cleanup within the EPQ EState's query context
- After cleanup, the EPQState is marked as idle with all key pointers set to NULL
- The function preserves resources shared with the parent query while cleaning up EPQ-specific allocations
- Used extensively in logical replication worker processes and executor nodes that support EPQ