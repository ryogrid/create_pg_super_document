# ExecClearTuple

## Location
[src/include/executor/tuptable.h:454-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L454-L471)

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
  - [TupleTableSlot](../T/TupleTableSlot.md) (struct type)
  - tts_ops->clear (function pointer)
- Called from (representative examples):
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [heap_getnextslot](../h/heap_getnextslot.md)
  - [CopyMultiInsertBufferFlush](../C/CopyMultiInsertBufferFlush.md)
  - [ExecScanFetch](ExecScanFetch.md)
  - [ExecResetTupleTable](ExecResetTupleTable.md)
  - [process_ordered_aggregate_multi](../p/process_ordered_aggregate_multi.md)
  - [BitmapHeapNext](../B/BitmapHeapNext.md)
  - [ExecProject](ExecProject.md)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md)

## Notes and Other Information
- Always returns the same slot pointer that was passed in, allowing for convenient chaining of operations
- The clearing operation is type-specific and handled by the slot's tts_ops->clear function
- Used extensively throughout the executor system for slot cleanup and reuse
- Critical for proper memory management in query execution
- Part of the TupleTableSlot abstraction layer that provides uniform access to different tuple storage formats
- Commonly used before storing new data in a slot or when resetting executor node state