# ExecAllocTableSlot

## Location
[src/backend/executor/execTuples.c:1258-1277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1258-L1277)

## Overview
Creates a new TupleTableSlot and adds it to a tuple table (managed as a List), providing a convenient way to allocate and track slots within the executor framework.

## Definition
```c
TupleTableSlot *
ExecAllocTableSlot(List **tupleTable, TupleDesc desc,
                   const TupleTableSlotOps *tts_ops)
```

## Detailed Description
This function serves as a convenience wrapper around MakeTupleTableSlot that additionally manages the slot within a tuple table structure. In PostgreSQL's executor, tuple tables are implemented as simple Lists that track all allocated slots for a particular execution context.

The function creates a new slot using the specified descriptor and operations, then appends it to the provided tuple table list. This integration allows the executor to maintain proper lifecycle management of all slots, enabling batch cleanup and resource management operations.

This is particularly useful in executor nodes that need to track multiple slots and ensure they are properly cleaned up when the execution context is destroyed.

## Parameters / Member Variables
- `tupleTable`: Pointer to a List pointer representing the tuple table to add the slot to
- `desc`: TupleDesc for the slot (can be NULL for flexible slots)
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the slot type

## Dependencies
- Functions called/Symbols referenced:
  - [MakeTupleTableSlot](../M/MakeTupleTableSlot.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [ExecInitResultSlot](ExecInitResultSlot.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md)
  - [find_hash_columns](../f/find_hash_columns.md)
  - [ExecInitIndexOnlyScan](ExecInitIndexOnlyScan.md)

## Notes and Other Information
- This is a thin wrapper that combines slot creation with tuple table management
- The tuple table parameter is modified in-place by appending the new slot
- Commonly used during executor node initialization to set up required slots
- The tuple table List serves as a registry for cleanup purposes during plan destruction
- All slots in a tuple table can be cleaned up together using ExecResetTupleTable
- This function maintains the same optimization benefits as MakeTupleTableSlot for fixed descriptors