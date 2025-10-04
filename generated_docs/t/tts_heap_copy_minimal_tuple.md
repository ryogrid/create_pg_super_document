# tts_heap_copy_minimal_tuple

## Location
[src/backend/executor/execTuples.c:475-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L475-L485)

## Overview
Creates a minimal tuple copy from a heap tuple table slot, converting the slot's heap tuple into a more compact MinimalTuple representation.

## Definition
```c
static MinimalTuple tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function is a specialized tuple table slot operation that extracts and converts a heap tuple into a MinimalTuple format. It first ensures that the heap tuple table slot contains a materialized tuple by calling tts_heap_materialize if needed, then uses minimal_tuple_from_heap_tuple to create the compact representation. This is particularly useful when a more memory-efficient tuple representation is needed while preserving the tuple data.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that should be a HeapTupleTableSlot containing the heap tuple to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleTableSlot](../H/HeapTupleTableSlot.md) (type cast)
  - [tts_heap_materialize](tts_heap_materialize.md)
  - [minimal_tuple_from_heap_tuple](../m/minimal_tuple_from_heap_tuple.md)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function specific to heap tuple table slot operations
- The function ensures the slot is materialized before attempting to copy the minimal tuple
- Part of the tuple table slot abstraction layer in PostgreSQL's executor
- Located in src/backend/executor/execTuples.c:475-485

## Simplified Source

```c
static MinimalTuple
tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
    HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

    // Ensure tuple is materialized before conversion
    if (!hslot->tuple)
        tts_heap_materialize(slot);

    // Convert heap tuple to compact minimal tuple format
    return minimal_tuple_from_heap_tuple(hslot->tuple);
}
```