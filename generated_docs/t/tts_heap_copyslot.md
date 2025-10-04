# tts_heap_copyslot

## Location
[src/backend/executor/execTuples.c:438-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L438-L450)

## Overview
Copies the contents of one tuple table slot to another heap tuple table slot, creating a materialized heap tuple in the destination slot's memory context.

## Definition
```c
static void tts_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
```

## Detailed Description
This function performs a complete copy operation between tuple table slots, where the destination must be a heap tuple table slot. It extracts a heap tuple from the source slot (regardless of the source slot type) and stores it as a materialized heap tuple in the destination slot. The copying process ensures that the destination slot owns its tuple data by allocating it in the destination slot's memory context.

The function leverages the existing ExecCopySlotHeapTuple and ExecStoreHeapTuple infrastructure to handle the complexities of cross-slot-type copying and proper memory management. The destination slot will have the shouldfree flag set, indicating it owns and should deallocate the tuple when cleared.

## Parameters / Member Variables
- `dstslot`: The destination TupleTableSlot where the copied heap tuple will be stored
- `srcslot`: The source TupleTableSlot from which data will be copied (can be any slot type)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCopySlotHeapTuple](../E/ExecCopySlotHeapTuple.md) (extracts/creates heap tuple from source slot)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md) (stores heap tuple into destination slot)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function is declared static, making it internal to the execTuples.c compilation unit
- Memory allocation occurs in the destination slot's memory context to ensure proper ownership
- The destination slot will be configured with shouldfree=true, making it responsible for tuple cleanup
- Part of the heap-specific tuple table slot operations infrastructure
- Handles cross-slot-type copying by relying on the polymorphic ExecCopySlotHeapTuple function

## Simplified Source

```c
static void
tts_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
    // Switch to destination slot's memory context for tuple allocation
    MemoryContext oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);

    // Extract heap tuple from source slot (handles any slot type)
    HeapTuple tuple = ExecCopySlotHeapTuple(srcslot);

    MemoryContextSwitchTo(oldcontext);

    // Store the copied tuple in destination slot with ownership transfer
    ExecStoreHeapTuple(tuple, dstslot, true);
}
```