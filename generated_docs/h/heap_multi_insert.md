# heap_multi_insert

## Location
src/backend/access/heap/heapam.c: 2309 - 2672

## Overview
heap_multi_insert efficiently inserts multiple tuples into a heap relation in one operation, optimizing performance by batching WAL records and minimizing page lock operations when multiple tuples fit on the same page.

## Definition


## Detailed Description
This function is an optimized version of heap_insert() for inserting multiple tuples simultaneously. It processes tuples by first preparing them all through heap_prepare_insert(), then inserting them page by page to minimize I/O operations. When multiple tuples fit on a single page, it writes only one WAL record covering all tuples and locks/unlocks the page once. The function handles serializable conflict detection, visibility map updates, logical decoding requirements, and proper transaction logging. It also manages relation extension by calculating required pages in advance using heap_multi_insert_pages().

## Parameters / Member Variables
- : The target heap relation for tuple insertion
- : Array of TupleTableSlot pointers containing the tuples to insert
- : Number of tuples to insert from the slots array
- : Command ID for the current command within the transaction
- : Insertion options flags (e.g., HEAP_INSERT_FROZEN)
- : Bulk insert state for optimizing buffer management

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionId
  - heap_prepare_insert
  - heap_multi_insert_pages
  - RelationGetBufferForTuple
  - RelationPutHeapTuple
  - CheckForSerializableConflictIn
  - ExecFetchSlotHeapTuple
  - visibilitymap_clear
  - visibilitymap_set
  - XLogInsert (and related WAL functions)
- Called from:
  - CatalogTuplesMultiInsertWithInfo
  - Various bulk insert operations

## Notes and Other Information
- This function leaks memory into the current memory context; create a temporary context if needed
- Currently does not support HEAP_INSERT_NO_LOGICAL option
- Performs serializable conflict checks both before and after insertion for correctness
- Handles visibility map updates for frozen and all-visible pages
- Supports logical decoding by including necessary tuple data and CID logging
- Uses critical sections to ensure atomicity of page modifications
- Optimizes relation extension by pre-calculating required pages
- Updates statistics via pgstat_count_heap_insert() upon completion