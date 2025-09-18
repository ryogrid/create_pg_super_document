# ExecBRUpdateTriggersNew

## Location
src/backend/commands/trigger.c: 2982 - 3146

## Overview
Executes BEFORE ROW UPDATE triggers for a new tuple, handling concurrent updates through EPQ (EvalPlanQual) rechecking and managing trigger execution workflow with proper memory management.

## Definition


## Detailed Description
This function executes BEFORE ROW UPDATE triggers for update operations, providing robust handling of concurrent updates through EPQ (EvalPlanQual) mechanisms. It retrieves the old tuple either from disk (using tupleid) or from FDW-supplied data (fdw_trigtuple), prepares trigger data structures, and iterates through all applicable BEFORE UPDATE triggers. The function handles memory management carefully, materializing slots when necessary to prevent dangling references, and supports both regular UPDATE and MERGE UPDATE operations with different EPQ behaviors.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : EPQ state for handling concurrent tuple modifications
- : Relation information including trigger descriptors and metadata
- : ItemPointer to the target tuple on disk (NULL if using fdw_trigtuple)
- : Pre-supplied tuple from FDW (NULL if using tupleid)
- : TupleTableSlot containing the new tuple values after update
- : Output parameter for tuple manager operation result
- : Output parameter for tuple manager failure data
- : Flag indicating if this is a MERGE UPDATE (affects EPQ behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md)
  - [ExecUpdateLockMode](ExecUpdateLockMode.md)
  - [GetTupleForTrigger](../G/GetTupleForTrigger.md)
  - [ExecGetUpdateNewTuple](ExecGetUpdateNewTuple.md)
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - ExecMaterializeSlot
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ExecBRUpdateTriggers](ExecBRUpdateTriggers.md)
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)

## Notes and Other Information
- Returns false if any trigger cancels the operation or if tuple retrieval fails
- Handles EPQ rechecking for concurrent updates, but skips it for MERGE UPDATE operations
- Properly manages memory by tracking which HeapTuples need to be freed
- Materializes slots when necessary to prevent references to unpinned buffers
- Supports both disk-based tuples (via tupleid) and FDW-supplied tuples (via fdw_trigtuple)
- Updates newslot in-place when EPQ processing provides a newer tuple version