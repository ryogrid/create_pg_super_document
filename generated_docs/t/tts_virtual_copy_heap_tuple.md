# tts_virtual_copy_heap_tuple

## Location
src/backend/executor/execTuples.c: 291 - 300

## Overview
Creates a HeapTuple from a VirtualTupleTableSlot by forming a tuple from the slot's values and null indicators using the slot's tuple descriptor.

## Definition
```c
static HeapTuple tts_virtual_copy_heap_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function converts a VirtualTupleTableSlot into a HeapTuple, which is PostgreSQL's standard on-disk tuple format. Virtual slots store attribute values as separate arrays in memory, while HeapTuples store data in a compact, serialized format suitable for storage and transmission.

The function delegates the actual tuple construction to `heap_form_tuple()`, which handles the complex process of:
- Organizing attribute values according to the tuple descriptor
- Applying proper alignment and padding
- Creating the tuple header with null bitmap
- Producing a self-contained HeapTuple structure

This conversion is typically needed when data from a virtual slot needs to be written to disk, sent over the network, or interfaced with code that expects HeapTuple format.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer (expected to be a VirtualTupleTableSlot) containing the data to convert

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - heap_form_tuple (function to create HeapTuple from values and null indicators)
  - MinimalTuple (type referenced, likely in nearby code)
- Called from (representative examples):
  - slot_deform_heap_tuple (at src/backend/executor/execTuples.c:1125)

## Notes and Other Information
- The function includes an assertion to ensure the slot is not empty before proceeding
- This is a static function, only accessible within execTuples.c
- The returned HeapTuple is allocated in the current memory context and must be freed by the caller
- The conversion process may involve data copying and reformatting depending on the attribute types
- Virtual slots are optimized for in-memory operations while HeapTuples are optimized for storage