# tts_buffer_heap_clear

## Location
src/backend/executor/execTuples.c: 719 - 748

## Overview
Clears a buffer-backed heap tuple table slot by releasing resources, freeing materialized tuples, and resetting the slot to an empty state.

## Definition
static void tts_buffer_heap_clear(TupleTableSlot *slot)

## Detailed Description
tts_buffer_heap_clear is responsible for cleaning up and resetting a buffer-backed heap tuple table slot. The function handles two main scenarios: materialized tuples that need to be freed, and buffer references that need to be released. For materialized tuples (created by copying data from the buffer), the function calls heap_freetuple to free the memory, but only if the TTS_SHOULDFREE flag is set. For buffer references, it calls ReleaseBuffer to decrement the buffer's reference count. Finally, it resets all slot state including clearing validity flags, setting the slot as empty, invalidating the tuple identifier, and resetting internal pointers.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot to be cleared. Expected to be of BufferHeapTupleTableSlot type.

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast target type)
  - TTS_SHOULDFREE (flag check macro)
  - [heap_freetuple](../h/heap_freetuple.md) (tuple memory deallocation)
  - TTS_FLAG_SHOULDFREE (flag manipulation)
  - ReleaseBuffer (buffer reference management)
  - TTS_FLAG_EMPTY (slot state flag)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md) (tuple ID invalidation)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function, accessible only within execTuples.c
- The function distinguishes between tuples that come directly from buffers (which cannot be freed) and materialized copies (which can be freed)
- Includes an assertion to ensure that materialized tuples have had their buffers unpinned
- Part of the tuple table slot operations vtable pattern
- Handles both memory management (via heap_freetuple) and buffer management (via ReleaseBuffer)
- Resets the slot to a clean, empty state suitable for reuse