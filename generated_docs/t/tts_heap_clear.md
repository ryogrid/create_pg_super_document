# tts_heap_clear

## Location
src/backend/executor/execTuples.c: 326 - 344

## Overview
tts_heap_clear clears the contents of a HeapTupleTableSlot, freeing associated memory and resetting the slot to an empty state while preserving the tuple descriptor.

## Definition
```c
static void
tts_heap_clear(TupleTableSlot *slot)
```

## Detailed Description
This function implements the clear callback for heap tuple table slots within the TupleTableSlotOps interface. It performs a complete cleanup of the slot's contents including freeing the heap tuple if the TTS_FLAG_SHOULDFREE flag is set, clearing attribute validity counters, marking the slot as empty, invalidating the tuple identifier, and resetting internal heap-specific state. This function is essential for slot reuse and memory management in PostgreSQL's tuple processing system.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot to be cleared (cast to HeapTupleTableSlot internally)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (cast target type)
  - TTS_SHOULDFREE (macro to check if tuple should be freed)
  - heap_freetuple (function to free heap tuple memory)
  - TTS_FLAG_SHOULDFREE (flag indicating tuple memory ownership)
  - TTS_FLAG_EMPTY (flag indicating empty slot state)
  - ItemPointerSetInvalid (function to invalidate tuple identifier)
- Called from (representative examples):
  - tts_heap_store_tuple
  - slot_deform_heap_tuple (indirectly through TupleTableSlotOps structure)

## Notes and Other Information
- This function properly manages memory by checking the TTS_SHOULDFREE flag before calling heap_freetuple
- Resets both generic slot fields (tts_nvalid, tts_flags, tts_tid) and heap-specific fields (off, tuple)
- The slot becomes reusable after clearing but retains its tuple descriptor
- Part of the standard slot lifecycle management in PostgreSQL's executor
- Critical for preventing memory leaks in tuple processing operations