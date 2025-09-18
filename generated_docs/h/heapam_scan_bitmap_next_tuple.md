# heapam_scan_bitmap_next_tuple

## Location
[src/backend/access/heap/heapam_handler.c:2255-2305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2255-L2305)

## Overview
Retrieves the next visible tuple during a bitmap heap scan, populating a tuple slot with the tuple data from the current block.

## Definition


## Detailed Description
This function works in conjunction with heapam_scan_bitmap_next_block to complete the bitmap scan process. After heapam_scan_bitmap_next_block identifies visible tuples on a block, this function retrieves them one by one. It handles the special case of "empty tuples" (when tuples don't need to be fetched), manages the scan cursor through visible tuples, constructs heap tuple structures, and populates the output slot. The function maintains proper buffer management and statistics reporting.

## Parameters / Member Variables
- : The table scan descriptor containing scan state and parameters
- : Bitmap iterator result (currently processed block information)  
- : Output tuple slot to be populated with the next tuple

## Dependencies
- Functions called/Symbols referenced:
  - [ExecStoreAllNullTuple](../E/ExecStoreAllNullTuple.md) (for empty tuple optimization)
  - [BufferGetPage](../B/BufferGetPage.md) (buffer management)
  - [PageGetItemId](../P/PageGetItemId.md) (page item access)
  - ItemIdIsNormal (item validation)
  - [PageGetItem](../P/PageGetItem.md) (tuple data retrieval)
  - ItemIdGetLength (tuple size)
  - [ItemPointerSet](../I/ItemPointerSet.md) (tuple identifier)
  - pgstat_count_heap_fetch (statistics)
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md) (slot population)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (as part of table access method interface)

## Notes and Other Information
- Works as the second phase of bitmap scanning after heapam_scan_bitmap_next_block
- Handles the "empty tuples" optimization where tuple data isn't needed (SO_NEED_TUPLES flag)
- Maintains scan position through rs_cindex cursor in visible tuples array
- Constructs complete HeapTupleData structures with proper metadata
- Updates scan statistics via pgstat_count_heap_fetch
- Returns false when all tuples on current block have been processed
- The populated slot acquires a buffer pin for tuple data access safety