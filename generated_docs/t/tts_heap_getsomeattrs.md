# tts_heap_getsomeattrs

## Location
[src/backend/executor/execTuples.c:345-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L345-L354)

## Overview
tts_heap_getsomeattrs extracts and makes accessible the first N attributes from a HeapTupleTableSlot by deforming the underlying heap tuple data.

## Definition
```c
static void
tts_heap_getsomeattrs(TupleTableSlot *slot, int natts)
```

## Detailed Description
This function implements the getsomeattrs callback for heap tuple table slots within the TupleTableSlotOps interface. It ensures that the specified number of attributes are extracted from the heap tuple and made available in the slot's tts_values and tts_isnull arrays. The function performs an assertion check to ensure the slot is not empty, then delegates the actual tuple deforming work to slot_deform_heap_tuple, which handles the low-level attribute extraction process incrementally and efficiently.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot containing the heap tuple (cast to HeapTupleTableSlot internally)
- `natts`: Number of attributes to extract and make accessible (must be ≤ tuple descriptor natts)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (cast target type)
  - TTS_EMPTY (macro to check if slot is empty)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (function that performs the actual attribute extraction)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (indirectly through TupleTableSlotOps structure)

## Notes and Other Information
- This function is part of PostgreSQL's lazy attribute extraction system for performance optimization
- The Assert(!TTS_EMPTY(slot)) ensures the slot contains valid tuple data before processing
- Uses the hslot->off field to track deforming progress across multiple calls
- Only extracts attributes up to the requested count, allowing for efficient partial tuple access
- The actual deforming logic is handled by slot_deform_heap_tuple for code reuse
- Critical for query execution performance when only subset of attributes are needed