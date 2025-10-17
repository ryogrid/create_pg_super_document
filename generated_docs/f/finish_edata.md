# finish_edata

## Location
[src/backend/replication/logical/worker.c:711-741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L711-L741)

## Overview
Performs cleanup and finalization operations for executor state data (ApplyExecutionData) created by create_edata_for_relation(), handling AFTER triggers, tuple routing cleanup, and proper resource deallocation.

## Definition
```c
static void
finish_edata(ApplyExecutionData *edata)
```

## Detailed Description
This function serves as the cleanup counterpart to create_edata_for_relation(), responsible for properly finalizing and releasing all resources associated with an ApplyExecutionData structure. The function performs several critical cleanup operations:

1. Handles any queued AFTER triggers by calling AfterTriggerEndQuery()
2. Cleans up tuple routing infrastructure if it was used during execution
3. Resets the tuple table to release slot resources
4. Frees the executor state and associated memory

The function deliberately avoids calling ExecCloseResultRelations() because the relation was added to es_opened_result_relations without taking a corresponding reference count. Instead, it relies on ExecCleanupTupleRouting() to close any additional relations that were opened during execution.

## Parameters / Member Variables
- `edata`: An ApplyExecutionData pointer containing the executor state and associated resources to be cleaned up and freed

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEndQuery](../A/AfterTriggerEndQuery.md)
  - [ExecCleanupTupleRouting](../E/ExecCleanupTupleRouting.md)
  - [ExecResetTupleTable](../E/ExecResetTupleTable.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- The function is designed to be called after all apply operations for a relation are complete
- Intentionally does not call ExecCloseResultRelations() due to reference count management considerations
- Proper cleanup order is important: triggers first, then tuple routing, then tuple table, then executor state
- The function handles both simple cases (no tuple routing) and complex cases (with tuple routing via mtstate and proute)
- Memory allocated for the ApplyExecutionData structure itself is freed using pfree()
- Must be paired with create_edata_for_relation() to ensure proper resource management

## Simplified Source

```c
static void finish_edata(ApplyExecutionData *edata) {
    EState *estate = edata->estate;

    // Handle any queued AFTER triggers
    AfterTriggerEndQuery(estate);

    // Clean up tuple routing if it was used
    if (edata->proute)
        ExecCleanupTupleRouting(edata->mtstate, edata->proute);

    // Clean up executor state and free memory
    ExecResetTupleTable(estate->es_tupleTable, false);
    FreeExecutorState(estate);
    pfree(edata);
}
```