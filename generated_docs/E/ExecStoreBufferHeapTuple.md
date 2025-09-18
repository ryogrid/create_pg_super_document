# ExecStoreBufferHeapTuple

## Location
src/backend/executor/execTuples.c: 1479 - 1504

## Overview
Stores an on-disk physical tuple from a buffer into a specified TTSOpsBufferHeapTuple type slot, maintaining buffer pin for tuple stability.

## Definition


## Detailed Description
ExecStoreBufferHeapTuple is specialized for storing on-disk physical tuples that reside in disk buffers into TTSOpsBufferHeapTuple type slots. The function is optimized for accessing tuples directly from disk pages without copying them into memory unnecessarily. It acquires and maintains a pin on the buffer to ensure the tuple remains valid until the slot is cleared.

This function is critical for efficient disk-based tuple access patterns where tuples are accessed directly from buffer pool pages. The buffer pin mechanism ensures data consistency by preventing the buffer from being evicted while the slot references the tuple data.

## Parameters
- : HeapTuple to store (pointing to data in the buffer)
- : TupleTableSlot of TTSOpsBufferHeapTuple type to store the tuple in  
- : Disk buffer containing the tuple data (must be valid, not InvalidBuffer)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - TTS_IS_BUFFERTUPLE
  - [tts_buffer_heap_store_tuple](../t/tts_buffer_heap_store_tuple.md)

- Called from (representative examples):
  - [heap_getnextslot](../h/heap_getnextslot.md)
  - [heap_getnextslot_tidrange](../h/heap_getnextslot_tidrange.md)
  - [heapam_index_fetch_tuple](../h/heapam_index_fetch_tuple.md)
  - [heapam_scan_analyze_next_tuple](../h/heapam_scan_analyze_next_tuple.md)
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [heapam_scan_bitmap_next_tuple](../h/heapam_scan_bitmap_next_tuple.md)

## Notes and Other Information
- The tuple table code acquires a pin on the buffer which is held until the slot is cleared
- Buffer pin ensures the tuple won't be evicted while referenced by the slot
- Requires a valid buffer - InvalidBuffer will cause assertion failure
- For non-guaranteed TTSOpsBufferHeapTuple slots, use ExecForceStoreHeapTuple() instead
- Extensively used in heap access methods for efficient on-disk tuple access
- Critical for scan operations, index lookups, and sampling that work directly with disk buffers
- Returns the passed-in slot pointer for convenience
- Part of PostgreSQL's buffer-aware tuple slot system for optimal memory usage