# ExecResetTupleTable

## Location
[src/backend/executor/execTuples.c:1278-1324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1278-L1324)

## Overview
Releases all resources held by slots in a tuple table and optionally frees the memory occupied by both the slots and the table structure itself.

## Definition
```c
void
ExecResetTupleTable(List *tupleTable,  /* tuple table */
                    bool shouldFree)   /* true if we should free memory */
```

## Detailed Description
This function serves as the primary cleanup mechanism for tuple tables in PostgreSQL's executor framework. It iterates through all slots in the provided tuple table and performs comprehensive resource cleanup for each slot.

The function operates in two phases:
1. **Resource Release**: For each slot, it clears any tuple data, calls the slot's type-specific release function, and releases tuple descriptor references
2. **Memory Deallocation**: When shouldFree is true, it frees the memory occupied by the slot structures themselves and the list structure

The function handles different slot types appropriately, distinguishing between fixed slots (allocated in a single block) and flexible slots (with separately allocated arrays). This ensures proper memory management regardless of how the slots were originally created.

## Parameters / Member Variables
- `tupleTable`: List containing TupleTableSlot pointers to be cleaned up
- `shouldFree`: Boolean flag indicating whether to free memory structures (true) or just release resources (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md)
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md)->release
  - ReleaseTupleDesc
  - TTS_FIXED (macro)
  - [pfree](../p/pfree.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md)
  - [ExecEndPlan](ExecEndPlan.md)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md)
  - [finish_edata](../f/finish_edata.md)

## Notes and Other Information
- Expected to be called by ExecEndPlan() during query execution cleanup
- Handles both fixed slots (single allocation) and flexible slots (separate arrays) correctly
- The shouldFree parameter allows for partial cleanup scenarios where slots might be reused
- Ensures proper reference counting for tuple descriptors through ReleaseTupleDesc
- Delegates type-specific cleanup to each slot's release function
- Critical for preventing memory leaks in long-running queries and complex execution plans
- The function is safe to call on empty or NULL tuple tables