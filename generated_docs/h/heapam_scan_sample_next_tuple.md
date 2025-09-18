# heapam_scan_sample_next_tuple

## Location
src/backend/access/heap/heapam_handler.c: 2396 - 2512

## Overview
Retrieves the next sampled tuple from the current page during a heap sample scan, applying visibility checks and tuple sampling logic.

## Definition


## Detailed Description
This function works with heapam_scan_sample_next_block to implement tuple-level sampling during sample scans. It uses the Table Sampling Method (TSM) API to determine which tuples on the current page should be examined, performs visibility checks on selected tuples, and handles both pagemode and non-pagemode scanning. The function manages buffer locking appropriately, performs serializable isolation checks when needed, and maintains scan statistics. It continues sampling tuples from the current page until a visible tuple is found or the page is exhausted.

## Parameters / Member Variables
- : The table scan descriptor containing scan state and configuration
- : Sample scan state containing sampling method information and parameters
- : Output tuple slot to be populated with the sampled tuple

## Dependencies
- Functions called/Symbols referenced:
  - TsmRoutine.NextSampleTuple (sampling method for tuple selection)
  - LockBuffer/BUFFER_LOCK_SHARE/BUFFER_LOCK_UNLOCK (buffer locking)
  - PageIsAllVisible (visibility optimization)
  - PageGetMaxOffsetNumber (page bounds)
  - PageGetItemId/ItemIdIsNormal (item access and validation)
  - PageGetItem/ItemIdGetLength (tuple data access)
  - ItemPointerSet (tuple identifier setup)
  - SampleHeapTupleVisible (visibility checking)
  - HeapCheckForSerializableConflictOut (isolation checks)
  - ExecStoreBufferHeapTuple (slot population)
  - pgstat_count_heap_getnext (statistics)
  - ExecClearTuple (slot cleanup)
- Called from (representative examples):
  - SampleHeapTupleVisible (as part of table access method interface)

## Notes and Other Information
- Implements the second phase of sample scanning after heapam_scan_sample_next_block
- Uses pluggable sampling methods through TSM API to determine which tuples to examine
- Supports both pagemode and non-pagemode operation with different locking strategies
- In pagemode, heap_prepare_pagescan pre-processes the page for efficiency
- In non-pagemode, manages buffer locks manually during visibility checks  
- Optimizes for all-visible pages to skip individual visibility checks
- Handles serializable isolation conflict detection when required
- Returns false when no more tuples are available on the current page
- Maintains proper tuple statistics via pgstat_count_heap_getnext
- Includes interrupt checking to allow cancellation during long sampling operations