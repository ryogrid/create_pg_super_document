# tts_minimal_copyslot

## Location
src/backend/executor/execTuples.c: 634 - 646

## Overview
Copies the contents of a source TupleTableSlot into a destination minimal tuple table slot, creating a new minimal tuple in the destination slot's memory context.

## Definition
```c
static void tts_minimal_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
```

## Detailed Description
This function implements the slot copy operation specifically for minimal tuple table slots. It performs a complete copy of a tuple from any type of source slot into a minimal tuple slot, ensuring the copied data is allocated in the destination slot's memory context.

The function works in two main steps:
1. Extract a minimal tuple representation from the source slot using `ExecCopySlotMinimalTuple`, which handles the conversion from any slot type to a minimal tuple format
2. Store the minimal tuple in the destination slot using `ExecStoreMinimalTuple`, which properly initializes the destination slot to contain the copied tuple

The memory context switching ensures that the copied minimal tuple is allocated in the destination slot's memory context, making the destination slot the owner of the tuple data. This is important for proper memory management and ensuring the copied tuple has the correct lifetime.

## Parameters / Member Variables
- `dstslot`: Pointer to the destination TupleTableSlot where the copy will be stored
- `srcslot`: Pointer to the source TupleTableSlot to copy from

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (type definition)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - ExecCopySlotMinimalTuple (extracts minimal tuple from any slot type)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md) (stores minimal tuple in destination slot)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function, only accessible within execTuples.c
- The function can copy from any type of slot (heap, minimal, virtual, etc.) into a minimal tuple slot
- Memory context switching ensures proper allocation in the destination slot's context
- The `true` parameter to `ExecStoreMinimalTuple` indicates that the slot should take ownership of the minimal tuple
- This operation is commonly used when tuples need to be copied between different execution contexts
- The copy is a deep copy - the destination slot becomes independent of the source slot after the operation
- The function is part of the slot operations vtable for minimal tuple slots, allowing polymorphic slot copying operations