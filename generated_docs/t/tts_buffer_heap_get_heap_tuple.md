# tts_buffer_heap_get_heap_tuple

## Location
src/backend/executor/execTuples.c: 903 - 915

## Overview
Returns a HeapTuple from a BufferHeapTupleTableSlot, materializing the slot if necessary to ensure a persistent HeapTuple is available.

## Definition


## Detailed Description
This function provides access to the HeapTuple contained within a BufferHeapTupleTableSlot. It implements lazy materialization - if the slot contains only virtual tuple data (no actual HeapTuple), it calls tts_buffer_heap_materialize() to create a persistent HeapTuple from the slot's tts_values and tts_isnull arrays.

The function serves as part of the BufferHeapTupleTableSlot's virtual method table, providing a standardized way to extract HeapTuple data from the slot regardless of whether the data is currently in virtual or materialized form.

After this function returns, the caller can be confident that the slot contains a valid HeapTuple that will remain accessible as long as the slot exists and hasn't been cleared.

## Parameters / Member Variables
- : The TupleTableSlot from which to extract the HeapTuple (must be a BufferHeapTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast to access slot-specific fields)
  - TTS_EMPTY (assertion check to ensure slot is not empty)
  - tts_buffer_heap_materialize (materializes virtual tuples when needed)

- Called from (representative examples):
  - slot_deform_heap_tuple

## Notes and Other Information
- The function includes an assertion to ensure the slot is not empty before attempting to access tuple data
- The materialization is performed only when necessary (when bslot->base.tuple is NULL), providing an optimization for cases where the HeapTuple already exists
- This is a static function implementing part of the BufferHeapTupleTableSlot virtual method table in src/backend/executor/execTuples.c
- The returned HeapTuple may point into a buffer or may be a materialized copy, depending on the slot's current state
- Callers should not assume ownership of the returned HeapTuple - it remains owned by the slot