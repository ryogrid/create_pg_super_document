# ExecBRDeleteTriggersNew

## Location
src/backend/commands/trigger.c: 2694 - 2793

## Overview
Executes BEFORE ROW DELETE triggers with advanced concurrency handling, EPQ (EvalPlanQual) support, and special handling for MERGE operations.

## Definition
```c
bool ExecBRDeleteTriggersNew(EState *estate, EPQState *epqstate,
                            ResultRelInfo *relinfo,
                            ItemPointer tupleid,
                            HeapTuple fdw_trigtuple,
                            TupleTableSlot **epqslot,
                            TM_Result *tmresult,
                            TM_FailureData *tmfd,
                            bool is_merge_delete)
```

## Detailed Description
ExecBRDeleteTriggersNew is an enhanced version of BEFORE ROW DELETE trigger execution that provides comprehensive concurrency control and EPQ (EvalPlanQual) support. The function handles both regular tables and foreign tables, manages concurrent tuple updates through EPQ mechanisms, and provides special handling for MERGE DELETE operations.

The function first retrieves the target tuple either from the provided HeapTuple (for foreign tables) or by fetching it from disk using the tuple ID. It includes sophisticated logic to handle concurrent updates: if a tuple has been modified by another transaction, it can either recheck using EPQ or return the updated tuple to the caller for further processing.

For MERGE operations, the function skips EPQ rechecking and delegates additional validation to the caller, since MERGE may need to execute different actions based on the concurrent changes. Each trigger can veto the delete operation by returning NULL, which causes the function to return false and suppress the deletion.

## Parameters / Member Variables
- `estate`: Execution state containing transaction and query context information
- `epqstate`: EvalPlanQual state for handling concurrent tuple updates and maintaining read consistency
- `relinfo`: Information about the target relation including trigger descriptors and cached functions
- `tupleid`: ItemPointer identifying the tuple to delete (used for regular tables)
- `fdw_trigtuple`: Pre-fetched HeapTuple for foreign tables (mutually exclusive with tupleid)
- `epqslot`: Output parameter for returning concurrently updated tuple when EPQ detects changes
- `tmresult`: Output parameter indicating the result of tuple manager operations (success, updated, deleted, etc.)
- `tmfd`: Output parameter containing failure details when tuple manager operations fail
- `is_merge_delete`: Flag indicating this deletion is part of a MERGE operation, affecting EPQ behavior

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
  - [heap_freetuple](../h/heap_freetuple.md)
  - TRIGGER_TYPE_MATCHES
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Constants used:
  - TRIGGER_EVENT_DELETE
  - TRIGGER_EVENT_ROW
  - TRIGGER_EVENT_BEFORE
  - TRIGGER_TYPE_ROW
  - TRIGGER_TYPE_BEFORE
  - TRIGGER_TYPE_DELETE
  - LockTupleExclusive
- Data structures used:
  - [EPQState](EPQState.md)
  - TriggerDesc
  - TriggerData
  - Trigger
  - TM_Result
  - TM_FailureData
- Called from (representative examples):
  - [ExecBRDeleteTriggers](ExecBRDeleteTriggers.md)
  - [ExecDeletePrologue](ExecDeletePrologue.md)

## Notes and Other Information
- Returns false if any trigger cancels the delete or if concurrent updates require caller attention
- Supports both regular heap tables (using tupleid) and foreign tables (using fdw_trigtuple)
- Implements EPQ (EvalPlanQual) for handling concurrent updates in READ COMMITTED isolation
- Special handling for MERGE operations that may need to execute different actions based on concurrent changes
- Triggers execute immediately and can see the current tuple state before deletion
- Memory management handles both should_free tuples and trigger-returned tuples appropriately
- Uses ExecGetTriggerOldSlot for consistent slot management across trigger operations
- Concurrent update detection returns updated tuple to caller when epqslot parameter is provided
- MERGE operations skip EPQ rechecking, delegating additional validation to the caller