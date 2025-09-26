# ExecInitExtraTupleSlot

## Location
[src/backend/executor/execTuples.c:1918-1933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1918-L1933)

## Overview
Creates and returns a new tuple slot for special-purpose use, optionally initialized with a specific tuple descriptor.

## Definition
```c
TupleTableSlot *ExecInitExtraTupleSlot(EState *estate, TupleDesc tupledesc, const TupleTableSlotOps *tts_ops)
```

## Detailed Description
ExecInitExtraTupleSlot is designed for creating additional tuple slots beyond the standard result and scan slots. Unlike other slot initialization functions that store the slot in specific fields of plan states, this function returns the newly created slot for the caller to manage. If a tuple descriptor is provided, the slot will have a fixed descriptor; otherwise, the caller must set the descriptor later using ExecSetSlotDescriptor(). This function is commonly used for temporary slots, trigger handling, subplans, and other special-purpose tuple operations.

## Parameters / Member Variables
- `estate`: Pointer to the execution state containing the tuple table
- `tupledesc`: Tuple descriptor defining the structure of tuples (can be NULL)
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the operations for the tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - [ExecAllocTableSlot](ExecAllocTableSlot.md): Creates and adds the tuple slot to the tuple table
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md): Structure defining slot operations
- Called from (representative examples):
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md): Creating null tuple slots
  - [ExecGetTriggerOldSlot](ExecGetTriggerOldSlot.md): Trigger handling for old row values
  - [ExecGetTriggerNewSlot](ExecGetTriggerNewSlot.md): Trigger handling for new row values
  - [ExecInitAgg](ExecInitAgg.md): Aggregation node for temporary slots
  - [ExecInitSubPlan](ExecInitSubPlan.md): Subplan execution
  - [ExecInitWindowAgg](ExecInitWindowAgg.md): Window aggregation functions
  - Various replication and worker functions

## Notes and Other Information
- Returns the slot directly rather than storing it in a plan state structure
- Caller is responsible for managing the returned slot
- Can accept a NULL tuple descriptor for late binding of slot structure
- Used for specialized scenarios requiring additional tuple storage
- Essential for trigger operations, subplans, and complex executor nodes
- Located in src/backend/executor/execTuples.c:1918-1933