# end_tup_output

## Location
src/backend/executor/execTuples.c: 2420 - 2426

## Overview
Cleans up and deallocates a TupOutputState structure, properly shutting down the destination receiver and freeing associated resources.

## Definition
```c
void end_tup_output(TupOutputState *tstate)
```

## Detailed Description
end_tup_output is the cleanup function for the tuple output infrastructure, designed to be called when tuple output operations are complete. It performs the necessary cleanup sequence to properly tear down all resources associated with a TupOutputState.

The function performs three key operations:
1. Calls the destination receiver's shutdown function (rShutdown) to allow the destination to perform any necessary cleanup or finalization
2. Drops the single TupleTableSlot that was allocated during initialization, properly deallocating the slot and its resources
3. Frees the TupOutputState structure itself

Notably, the function does not destroy the DestReceiver itself, as that responsibility belongs to the caller who created or provided the destination receiver.

## Parameters / Member Variables
- `tstate`: TupOutputState pointer to be cleaned up, typically created by begin_tup_output_tupdesc

## Dependencies
- Functions called/Symbols referenced:
  - DestReceiver->rShutdown
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md) (basebackup_copy.c)
  - [SendTablespaceList](../S/SendTablespaceList.md) (basebackup_copy.c)
  - [ExplainQuery](../E/ExplainQuery.md) (explain.c)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md) (functioncmds.c)
  - [IdentifySystem](../I/IdentifySystem.md) (walsender.c)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (walsender.c)
  - ShowGUCConfigOption (guc_funcs.c)
  - ShowAllGUCConfig (guc_funcs.c)

## Notes and Other Information
- Should be called for every TupOutputState created with begin_tup_output_tupdesc to prevent resource leaks
- The DestReceiver is not destroyed by this function and remains the caller's responsibility
- Part of the complete tuple output lifecycle: begin_tup_output_tupdesc → do_tup_output (multiple calls) → end_tup_output
- Used consistently across utility commands that leverage the tuple output infrastructure
- Proper resource management ensures that all allocated memory and slots are correctly cleaned up