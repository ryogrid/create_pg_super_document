# tts_buffer_heap_materialize

## Location
[src/backend/executor/execTuples.c:802-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L802-L860)

## Overview
Materializes a BufferHeapTupleTableSlot by creating a persistent copy of its tuple data that can outlive the original buffer reference.

## Definition

```c
static void
tts_buffer_heap_materialize(TupleTableSlot *slot)
```
## Detailed Description
This function transforms a BufferHeapTupleTableSlot from a potentially transient state (where the tuple data might be tied to a buffer that could be released) into a materialized state where the tuple data is copied into the slot's own memory context. The materialization process ensures that the tuple data remains accessible even after the original buffer is released.

The function handles two main scenarios:
1. **Virtual tuples**: When the slot contains only virtual tuple data (tts_values/tts_isnull arrays), it creates a new HeapTuple using heap_form_tuple()
2. **Buffer-backed tuples**: When the slot already has a tuple backed by a buffer, it creates a copy using heap_copytuple() and releases the original buffer

The materialization is performed in the slot's memory context to ensure proper memory management, and the slot is marked with TTS_FLAG_SHOULDFREE to indicate ownership of the tuple data.

## Parameters / Member Variables
- : The TupleTableSlot to materialize, which must be a BufferHeapTupleTableSlot

## Dependencies
- Functions called/Symbols referenced:
  - [BufferHeapTupleTableSlot](../B/BufferHeapTupleTableSlot.md) (cast target)
  - TTS_EMPTY (macro to check if slot is empty)
  - TTS_SHOULDFREE (macro to check if slot owns its tuple)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates HeapTuple from values/nulls arrays)
  - [heap_copytuple](../h/heap_copytuple.md) (creates copy of existing HeapTuple)
  - likely (branch prediction hint macro)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (releases buffer reference)
  - TTS_FLAG_SHOULDFREE (flag indicating slot owns tuple memory)
  
- Called from (representative examples):
  - [tts_buffer_heap_get_heap_tuple](tts_buffer_heap_get_heap_tuple.md)
  - [tts_buffer_heap_copy_heap_tuple](tts_buffer_heap_copy_heap_tuple.md)
  - [tts_buffer_heap_copy_minimal_tuple](tts_buffer_heap_copy_minimal_tuple.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function includes an early return optimization if the slot is already materialized (TTS_SHOULDFREE flag is set)
- Buffer release is handled carefully - the TTS_FLAG_SHOULDFREE flag is only set after buffer release to maintain assertion invariants
- The function resets slot->tts_nvalid and bslot->base.off to 0, forcing re-deformation of the tuple to ensure all pointers reference the materialized data
- This is a static function in src/backend/executor/execTuples.c, part of the BufferHeapTupleTableSlot virtual method table implementation