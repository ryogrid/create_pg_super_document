# ExecClearTuple

## Location
src/include/executor/tuptable.h: 454 - 471

## Overview
Clears the contents of a TupleTableSlot, making it empty and ready for reuse.

## Definition
```c
static inline TupleTableSlot *
ExecClearTuple(TupleTableSlot *slot)
```

## Detailed Description
ExecClearTuple is a fundamental utility function in PostgreSQL's executor system that clears the contents of a TupleTableSlot. It delegates to the slot's type-specific clear operation through the tts_ops function pointer table, allowing different slot types to implement their own clearing logic. After clearing, the slot is in an empty state and can be reused for storing new tuples.

This function is essential for memory management and slot reuse in the executor system, preventing memory leaks and ensuring proper cleanup of slot contents. It's used extensively throughout the executor system when slots need to be reset or reused.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot (struct type)
  - tts_ops->clear (function pointer)
- Called from (representative examples):
  - execute_attr_map_slot
  - heap_getnextslot
  - CopyMultiInsertBufferFlush
  - ExecScanFetch
  - ExecResetTupleTable
  - process_ordered_aggregate_multi
  - BitmapHeapNext
  - ExecProject
  - tuplesort_gettupleslot

## Notes and Other Information
- Always returns the same slot pointer that was passed in, allowing for convenient chaining of operations
- The clearing operation is type-specific and handled by the slot's tts_ops->clear function
- Used extensively throughout the executor system for slot cleanup and reuse
- Critical for proper memory management in query execution
- Part of the TupleTableSlot abstraction layer that provides uniform access to different tuple storage formats
- Commonly used before storing new data in a slot or when resetting executor node state