# heap_freeze_prepared_tuples

## Location
src/backend/access/heap/heapam.c: 7359 - 7380

## Overview
Helper function that executes freezing of one or more heap tuples on a page, taking an array of prepared tuple freeze plans and applying them to the specified tuples.

## Definition
```c
void heap_freeze_prepared_tuples(Buffer buffer, HeapTupleFreeze *tuples, int ntuples)
```

## Detailed Description
This function serves as a helper that executes the actual freezing operation for multiple heap tuples on a single page. It takes an array of HeapTupleFreeze plans that have been prepared by `heap_prepare_freeze_tuple` and applies them to the corresponding tuples. The caller must set the 'offset' field in each plan to specify which tuple to freeze. This function must be called within a critical section that also marks the buffer as dirty and emits WAL records if needed.

The function iterates through each freeze plan, retrieves the corresponding tuple from the page using the offset, and calls `heap_execute_freeze_tuple` to perform the actual freezing operation on each tuple.

## Parameters / Member Variables
- `buffer`: The buffer containing the page with tuples to be frozen
- `tuples`: Array of HeapTupleFreeze structures containing freeze plans prepared by heap_prepare_freeze_tuple
- `ntuples`: Number of tuples in the array to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_execute_freeze_tuple](heap_execute_freeze_tuple.md)
- Types used:
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md)
  - ItemId
  - HeapTupleHeader
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information
- Must be called within a critical section
- Caller is responsible for marking the buffer dirty and emitting WAL if needed
- The 'offset' field in each HeapTupleFreeze plan must be set by the caller before calling this function
- This is a low-level function that performs the actual tuple modification after freeze planning is complete