# tts_buffer_heap_copy_minimal_tuple

## Location
[src/backend/executor/execTuples.c:929-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L929-L941)

## Overview
Creates a MinimalTuple copy from a BufferHeapTupleTableSlot by first ensuring the slot contains a HeapTuple and then converting it to the more compact MinimalTuple format.

## Definition


## Detailed Description
This function extracts tuple data from a BufferHeapTupleTableSlot and creates a MinimalTuple representation. MinimalTuples are a more compact representation compared to HeapTuples, as they remove HeapTuple-specific overhead and metadata that isn't needed in certain contexts.

The function operates in two phases:

1. **Materialization Phase**: If the slot contains only virtual tuple data (no HeapTuple), it calls tts_buffer_heap_materialize() to create a HeapTuple from the slot's tts_values and tts_isnull arrays.

2. **Conversion Phase**: Once a HeapTuple is available, it calls minimal_tuple_from_heap_tuple() to create the MinimalTuple, which copies the essential tuple data while removing HeapTuple-specific overhead.

The returned MinimalTuple is independent of the original slot and provides a memory-efficient representation suitable for contexts where the full HeapTuple metadata is not required.

## Parameters / Member Variables
- : The TupleTableSlot from which to create a MinimalTuple (must be a BufferHeapTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast to access slot-specific fields)
  - TTS_EMPTY (assertion check to ensure slot is not empty)
  - [tts_buffer_heap_materialize](tts_buffer_heap_materialize.md) (materializes virtual tuples when needed)
  - [minimal_tuple_from_heap_tuple](../m/minimal_tuple_from_heap_tuple.md) (converts HeapTuple to MinimalTuple format)

- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function includes an assertion to ensure the slot is not empty before attempting to access tuple data
- MinimalTuples are particularly useful in scenarios like sorting, hashing, or temporary storage where the full HeapTuple metadata is unnecessary overhead
- The materialization step only occurs when necessary (when bslot->base.tuple is NULL), providing optimization for cases where a HeapTuple already exists
- This is a static function implementing part of the BufferHeapTupleTableSlot virtual method table in src/backend/executor/execTuples.c
- The caller assumes ownership of the returned MinimalTuple and is responsible for freeing it with pfree() when no longer needed
- MinimalTuples created by this function are allocated in the current memory context
- This function leverages the related processed symbol minimal_tuple_from_heap_tuple, which handles the HeapTuple to MinimalTuple conversion by removing HeapTuple-specific overhead