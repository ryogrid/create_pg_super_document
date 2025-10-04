# tts_heap_copy_heap_tuple

## Location
[src/backend/executor/execTuples.c:463-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L463-L474)

## Overview
Creates and returns an independent copy of the HeapTuple stored in a heap tuple table slot, materializing the tuple first if necessary.

## Definition
```c
static HeapTuple tts_heap_copy_heap_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function extracts a HeapTuple from a HeapTupleTableSlot and creates an independent copy using heap_copytuple. If the slot contains only deconstructed values without a materialized tuple, it first calls tts_heap_materialize to ensure a physical tuple exists before copying. The returned tuple is allocated in the current memory context and is owned by the caller.

Unlike tts_heap_get_heap_tuple which returns a pointer to the slot's internal tuple, this function provides complete independence from the source slot. The copied tuple will persist even if the original slot is cleared or goes out of scope, making it suitable for scenarios where tuple data needs to outlive the slot.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer (must be a HeapTupleTableSlot) from which to copy the HeapTuple

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleTableSlot](../H/HeapTupleTableSlot.md) (cast target type)
  - TTS_EMPTY (macro for checking empty slots)
  - [tts_heap_materialize](tts_heap_materialize.md) (materializes tuple if needed)
  - [heap_copytuple](../h/heap_copytuple.md) (creates independent copy of heap tuple)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function is declared static, making it internal to the execTuples.c compilation unit
- Returns a completely independent copy allocated in the current memory context
- Automatically handles materialization if the slot contains only deconstructed values
- The caller is responsible for freeing the returned HeapTuple
- Part of the heap-specific tuple table slot operations infrastructure
- Used when tuple data needs to persist beyond the lifetime of the source slot

## Simplified Source

```c
static HeapTuple
tts_heap_copy_heap_tuple(TupleTableSlot *slot)
{
    HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

    // Ensure slot contains data
    Assert(!TTS_EMPTY(slot));

    // Materialize tuple if not already present
    if (!hslot->tuple)
        tts_heap_materialize(slot);

    // Return independent copy of the tuple
    return heap_copytuple(hslot->tuple);
}
```