# RelationPutHeapTuple

## Location
src/backend/access/heap/hio.c: 35 - 87

## Overview
RelationPutHeapTuple places a heap tuple at a specified page in a PostgreSQL buffer, handling the physical insertion of tuple data into heap pages with proper offset tracking and CTID management.

## Definition


## Detailed Description
This function performs the low-level physical insertion of a heap tuple into a buffer page. It is a critical component of PostgreSQL's heap access method, responsible for:
- Adding the tuple data to the specified page using PageAddItem
- Updating the tuple's t_self field with the actual storage position 
- Setting the correct CTID in the stored tuple header (unless it's a speculative insertion)
- Performing validation checks on tuple hint bits to prevent corruption

The function includes strict error handling - it must PANIC on failure rather than using EREPORT(ERROR), indicating this is used in contexts where partial failure is not acceptable. The caller must hold BUFFER_LOCK_EXCLUSIVE on the buffer before calling this function.

## Parameters / Member Variables
- : The relation (table) where the tuple is being inserted
- : The buffer containing the target page (caller must hold BUFFER_LOCK_EXCLUSIVE)
- : The heap tuple to be inserted into the page
- : Boolean flag indicating whether this is a speculative insertion (token held in CTID field)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)  
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - HeapTupleHeaderIsSpeculative
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Critical constraint: EREPORT(ERROR) is disallowed - must PANIC on failure
- Requires caller to hold BUFFER_LOCK_EXCLUSIVE on the buffer
- Validates tuple hint bits to prevent corruption detectable by contrib/amcheck
- Handles both regular and speculative insertions differently for CTID management
- Updates both tuple->t_self and the stored tuple's t_ctid fields for proper tuple chain management
- Failure to add tuple to page results in PANIC, indicating a serious system-level error