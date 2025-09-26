# slot_deform_heap_tuple

## Location
[src/backend/executor/execTuples.c:1008-1198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1008-L1198)

## Overview
Incrementally extracts attribute data from a HeapTuple into a TupleTableSlot's Datum/isnull arrays, optimizing performance by caching offset information and avoiding re-computation of previously extracted attributes.

## Definition
```c
static pg_attribute_always_inline void
slot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
                       int natts)
```

## Detailed Description
This function serves as the core tuple deforming mechanism in PostgreSQL's executor, providing an incremental version of heap_deform_tuple. It efficiently extracts attribute values from a physical tuple into the slot's Datum/isnull arrays, processing only the attributes that haven't been extracted yet.

The function implements several key optimizations:
- **Incremental processing**: Only extracts attributes beyond those already processed (tracked by tts_nvalid)
- **Offset caching**: Uses attcacheoff to avoid recalculating alignment offsets for fixed-length attributes
- **Alignment optimization**: Handles both nominal and pointer-based alignment for variable-length attributes
- **State preservation**: Maintains extraction state between calls to avoid redundant work

The function carefully handles NULL values, variable-length attributes (varlena), and alignment requirements while maintaining compatibility with different tuple formats and slot types.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to store extracted attribute values in
- `tuple`: The HeapTuple containing the physical tuple data to extract from
- `offp`: Pointer to the current offset within the tuple data (updated during processing)
- `natts`: Number of attributes to extract (limited by tuple's actual attribute count)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasNulls
  - HeapTupleHeaderGetNatts
  - TupleDescAttr
  - [att_isnull](../a/att_isnull.md)
  - att_align_nominal
  - att_align_pointer
  - fetchatt
  - att_addlength_pointer
  - TTS_SLOW (macro)
- Called from (representative examples):
  - [tts_heap_getsomeattrs](../t/tts_heap_getsomeattrs.md)
  - [tts_minimal_getsomeattrs](../t/tts_minimal_getsomeattrs.md)
  - [tts_buffer_heap_getsomeattrs](../t/tts_buffer_heap_getsomeattrs.md)

## Notes and Other Information
- Marked as pg_attribute_always_inline for performance optimization in hot code paths
- Implements the TTS_FLAG_SLOW mechanism to track when offset caching becomes invalid
- Handles both aligned and unaligned variable-length attributes efficiently
- The function is designed to work with different slot types through polymorphism
- Critical for query execution performance as it's called frequently during tuple processing
- State is preserved between calls via tts_nvalid and the offset pointer to enable incremental processing