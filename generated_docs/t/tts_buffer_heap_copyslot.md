# tts_buffer_heap_copyslot

## Location
src/backend/executor/execTuples.c: 861 - 902

## Overview
Copies the contents of one TupleTableSlot to another, optimizing for BufferHeapTupleTableSlots by sharing buffer references when possible rather than always materializing copies.

## Definition


## Detailed Description
This function implements slot copying logic specifically optimized for BufferHeapTupleTableSlots. It uses two different strategies based on the state of the source slot:

**Strategy 1 - Full Copy**: Used when the source slot is incompatible for buffer sharing:
- Source slot is of a different type (different tts_ops)
- Source slot is already materialized (TTS_SHOULDFREE flag set)
- Source slot contains virtual tuple data (no actual HeapTuple)

In this case, it creates a complete copy using ExecCopySlotHeapTuple() and marks the destination slot as owning the tuple memory.

**Strategy 2 - Buffer Reference Sharing**: Used when the source slot contains a buffer-backed HeapTuple that can be shared:
- Both slots are BufferHeapTupleTableSlots
- Source slot is not materialized and contains an actual HeapTuple
- Source slot has a valid buffer reference

In this case, it shares the buffer reference via tts_buffer_heap_store_tuple() but creates a local copy of the HeapTupleData structure to ensure the destination slot's independence from the source slot's lifetime.

## Parameters / Member Variables
- : Destination TupleTableSlot to copy into (must be BufferHeapTupleTableSlot)
- : Source TupleTableSlot to copy from (typically BufferHeapTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast for both source and destination)
  - TTS_SHOULDFREE (checks if source slot owns its tuple)
  - ExecClearTuple (clears destination slot)
  - TTS_FLAG_EMPTY (flag indicating empty slot)
  - ExecCopySlotHeapTuple (creates HeapTuple copy from slot)
  - TTS_FLAG_SHOULDFREE (marks slot as owning tuple memory)
  - [tts_buffer_heap_store_tuple](tts_buffer_heap_store_tuple.md) (stores tuple with buffer reference)
  - [HeapTupleData](../H/HeapTupleData.md) (tuple header structure)

- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function includes an important optimization: when sharing buffer references, it copies the HeapTupleData structure locally to prevent use-after-free issues if the source slot is freed before the destination
- The buffer sharing optimization reduces memory allocation and copying overhead when both slots can reference the same underlying buffer page
- The function handles cross-slot-type copying by falling back to the full copy strategy when slot types differ
- This is a static function implementing part of the BufferHeapTupleTableSlot virtual method table in src/backend/executor/execTuples.c