# tts_buffer_heap_copy_heap_tuple

## Location
src/backend/executor/execTuples.c: 916 - 928

## Overview
Creates an independent copy of the HeapTuple contained in a BufferHeapTupleTableSlot, ensuring the caller owns the returned tuple and can use it beyond the lifetime of the original slot.

## Definition


## Detailed Description
This function extracts a HeapTuple from a BufferHeapTupleTableSlot and creates a complete, independent copy of it. The function implements a two-step process:

1. **Materialization Check**: If the slot contains only virtual tuple data (no HeapTuple), it first calls tts_buffer_heap_materialize() to create a HeapTuple from the slot's tts_values and tts_isnull arrays.

2. **Copy Creation**: Once a HeapTuple is available in the slot, it uses heap_copytuple() to create a complete copy that includes both the HeapTupleData header and all tuple data.

The returned HeapTuple is completely independent of the original slot and can be safely used even after the slot is cleared or freed. The caller assumes ownership of the returned tuple and is responsible for freeing it when no longer needed.

## Parameters / Member Variables
- : The TupleTableSlot from which to copy the HeapTuple (must be a BufferHeapTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast to access slot-specific fields)
  - TTS_EMPTY (assertion check to ensure slot is not empty)
  - tts_buffer_heap_materialize (materializes virtual tuples when needed)
  - heap_copytuple (creates independent copy of HeapTuple)
  - MinimalTuple (referenced in source context)

- Called from (representative examples):
  - slot_deform_heap_tuple

## Notes and Other Information
- The function includes an assertion to ensure the slot is not empty before attempting to access tuple data
- Unlike tts_buffer_heap_get_heap_tuple(), this function always returns a copy that the caller owns, providing memory safety for long-term storage
- The materialization step only occurs if necessary (when bslot->base.tuple is NULL), optimizing cases where the HeapTuple already exists
- This is a static function implementing part of the BufferHeapTupleTableSlot virtual method table in src/backend/executor/execTuples.c
- The returned HeapTuple is allocated in the current memory context and should be freed with heap_freetuple() when no longer needed
- This function is essential for scenarios where tuple data needs to outlive the original slot or buffer references