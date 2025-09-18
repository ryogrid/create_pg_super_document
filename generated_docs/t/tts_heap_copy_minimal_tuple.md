# tts_heap_copy_minimal_tuple

## Location
src/backend/executor/execTuples.c: 475 - 485

## Overview
Creates a minimal tuple copy from a heap tuple table slot, converting the slot's heap tuple into a more compact MinimalTuple representation.

## Definition
```c
static MinimalTuple tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function is a specialized tuple table slot operation that extracts and converts a heap tuple into a MinimalTuple format. It first ensures that the heap tuple table slot contains a materialized tuple by calling tts_heap_materialize if needed, then uses minimal_tuple_from_heap_tuple to create the compact representation. This is particularly useful when a more memory-efficient tuple representation is needed while preserving the tuple data.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that should be a HeapTupleTableSlot containing the heap tuple to be converted

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (type cast)
  - tts_heap_materialize
  - minimal_tuple_from_heap_tuple
- Called from (representative examples):
  - slot_deform_heap_tuple

## Notes and Other Information
- This is a static function specific to heap tuple table slot operations
- The function ensures the slot is materialized before attempting to copy the minimal tuple
- Part of the tuple table slot abstraction layer in PostgreSQL's executor
- Located in src/backend/executor/execTuples.c:475-485