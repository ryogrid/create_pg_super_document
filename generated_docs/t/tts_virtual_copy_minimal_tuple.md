# tts_virtual_copy_minimal_tuple

## Location
src/backend/executor/execTuples.c: 301 - 315

## Overview
Creates a MinimalTuple from a VirtualTupleTableSlot by forming a compact tuple representation from the slot's values and null indicators using the slot's tuple descriptor.

## Definition
```c
static MinimalTuple tts_virtual_copy_minimal_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function converts a VirtualTupleTableSlot into a MinimalTuple, which is PostgreSQL's most compact tuple format. MinimalTuples are even more space-efficient than HeapTuples as they contain only the essential data without some of the header information present in HeapTuples.

The function uses `heap_form_minimal_tuple()` to construct the MinimalTuple from the virtual slot's data arrays. MinimalTuples are particularly useful in scenarios where memory usage is critical, such as:
- Sorting operations with large datasets
- Hash tables and hash joins
- Temporary storage during query execution
- Network transmission where bandwidth is limited

The conversion process organizes the virtual slot's separate value and null arrays into the MinimalTuple's compact, serialized format according to the tuple descriptor's specifications.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer (expected to be a VirtualTupleTableSlot) containing the data to convert

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - [heap_form_minimal_tuple](../h/heap_form_minimal_tuple.md) (function to create MinimalTuple from values and null indicators)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (at src/backend/executor/execTuples.c:1126)

## Notes and Other Information
- The function includes an assertion to ensure the slot is not empty before proceeding
- This is a static function, only accessible within execTuples.c
- MinimalTuples are more memory-efficient than HeapTuples but lack some metadata like transaction information
- The returned MinimalTuple is allocated in the current memory context and must be freed by the caller
- Related to the provided summary of `heap_form_minimal_tuple`, this function leverages that symbol to create compact tuple representations without header information present in full HeapTuples
- MinimalTuples are particularly valuable in memory-constrained operations and temporary data structures