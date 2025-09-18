# ExecForceStoreHeapTuple

## Location
src/backend/executor/execTuples.c: 1556 - 1598

## Overview
Stores a HeapTuple into any kind of TupleTableSlot, performing automatic type conversion and memory management as necessary to accommodate different slot types.

## Definition
```c
void ExecForceStoreHeapTuple(HeapTuple tuple, TupleTableSlot *slot, bool shouldFree)
```

## Detailed Description
ExecForceStoreHeapTuple is a versatile function that can store a HeapTuple into any type of TupleTableSlot by automatically detecting the target slot type and performing appropriate conversions. Unlike type-specific storage functions, this function handles three different slot types:

1. **HeapTuple slots**: Uses the optimized ExecStoreHeapTuple for direct storage
2. **BufferTuple slots**: Creates a copy of the tuple in the slot's memory context
3. **Other slot types**: Deforms the tuple into individual column values and stores as a virtual tuple

The function manages memory correctly for each case, respecting the shouldFree parameter to determine whether the original tuple should be freed after storage.

## Parameters / Member Variables
- `tuple`: The HeapTuple to be stored in the slot
- `slot`: The target TupleTableSlot where the tuple will be stored (can be any slot type)
- `shouldFree`: Boolean flag indicating whether the original tuple should be freed after storage

## Dependencies
- Functions called/Symbols referenced:
  - TTS_IS_HEAPTUPLE (type checking macro)
  - [ExecStoreHeapTuple](ExecStoreHeapTuple.md) (optimized heap tuple storage)
  - TTS_IS_BUFFERTUPLE (type checking macro)
  - BufferHeapTupleTableSlot (buffer slot type)
  - ExecClearTuple (slot clearing function)
  - [heap_copytuple](../h/heap_copytuple.md) (tuple copying function)
  - [heap_deform_tuple](../h/heap_deform_tuple.md) (tuple deformation function)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md) (virtual tuple storage)
  - ExecMaterializeSlot (slot materialization)
- Called from (representative examples):
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md)
  - [ExecARDeleteTriggers](ExecARDeleteTriggers.md)
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [IndexOnlyNext](../I/IndexOnlyNext.md)
  - [ExecDelete](ExecDelete.md)
  - [ExecMergeMatched](ExecMergeMatched.md)

## Notes and Other Information
- This function provides a universal interface for storing heap tuples regardless of the target slot type
- Automatically handles memory management and type conversions transparently
- For buffer tuple slots, it creates a copy in the slot's memory context to ensure proper lifetime management
- For non-heap/non-buffer slots, it decomposes the tuple into individual column values for virtual storage
- More expensive than type-specific storage functions but provides maximum flexibility
- Essential for trigger execution and various executor nodes that need to work with different slot types
- When shouldFree is true and the slot type requires deformation, the function materializes the slot before freeing the original tuple to preserve data