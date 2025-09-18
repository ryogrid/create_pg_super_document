# tts_heap_get_heap_tuple

## Location
src/backend/executor/execTuples.c: 451 - 462

## Overview
Returns a HeapTuple from a heap tuple table slot, materializing the tuple if necessary to ensure a physical tuple exists.

## Definition
```c
static HeapTuple tts_heap_get_heap_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function provides access to the physical HeapTuple stored in a HeapTupleTableSlot. If the slot contains only deconstructed values without a materialized tuple, it automatically triggers materialization through tts_heap_materialize before returning the tuple. This ensures callers always receive a valid HeapTuple pointer.

The function serves as a safe accessor for the tuple field of HeapTupleTableSlot, handling the complexity of on-demand materialization transparently. It's designed to be called when code specifically needs access to the physical tuple structure rather than just the column values.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer (must be a HeapTupleTableSlot) from which to retrieve the HeapTuple

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (cast target type)
  - TTS_EMPTY (macro for checking empty slots)
  - [tts_heap_materialize](tts_heap_materialize.md) (materializes tuple if needed)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function is declared static, making it internal to the execTuples.c compilation unit
- Automatically materializes the tuple if not already present, ensuring the returned pointer is always valid
- Part of the heap-specific tuple table slot operations infrastructure
- Assumes the input slot is actually a HeapTupleTableSlot (performs unsafe cast)
- Used when callers need direct access to the HeapTuple structure for operations like tuple copying or low-level manipulation