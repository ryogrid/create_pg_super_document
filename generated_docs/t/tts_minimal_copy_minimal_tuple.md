# tts_minimal_copy_minimal_tuple

## Location
[src/backend/executor/execTuples.c:669-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L669-L679)

## Overview
Creates an independent copy of the MinimalTuple stored in a MinimalTupleTableSlot.

## Definition
static MinimalTuple tts_minimal_copy_minimal_tuple(TupleTableSlot *slot)

## Detailed Description
This function extracts the MinimalTuple from a MinimalTupleTableSlot and creates an independent copy of it. It ensures the slot contains a materialized minimal tuple by calling tts_minimal_materialize if necessary, then uses heap_copy_minimal_tuple to perform a byte-for-byte copy of the tuple data. The returned copy is allocated in the current memory context and is owned by the caller, providing isolation from the original slot's tuple data.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that must be a MinimalTupleTableSlot instance

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTupleTableSlot (struct type cast)
  - [tts_minimal_materialize](tts_minimal_materialize.md)
  - [heap_copy_minimal_tuple](../h/heap_copy_minimal_tuple.md)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function internal to execTuples.c
- Returns a newly allocated MinimalTuple that the caller must manage
- The copy is completely independent of the original slot's tuple
- Used when the caller needs a persistent copy that won't be affected by changes to the slot
- More efficient than converting to HeapTuple when minimal tuple format is sufficient