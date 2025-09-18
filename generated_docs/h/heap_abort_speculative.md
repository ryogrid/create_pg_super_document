# heap_abort_speculative

## Location
src/backend/access/heap/heapam.c: 6129 - 6307

## Overview
Kills a speculatively inserted tuple by marking it as immediately dead, preventing unprincipled deadlocks in high-concurrency scenarios.

## Definition
```c
void heap_abort_speculative(Relation relation, ItemPointer tid)
```

## Detailed Description
This function aborts a speculative insertion by making the tuple immediately visible as dead to all transactions, including the inserting transaction itself. The key operations include:

1. Setting the tuple's xmin to InvalidTransactionId, making it immediately invisible
2. Clearing the speculative insertion token from t_ctid 
3. Setting up page pruning hints for future cleanup
4. Logging the operation via WAL as a delete operation
5. Handling any associated TOAST data cleanup

The function prevents unprincipled deadlocks that could occur when multiple backends attempt speculative insertions of duplicate keys. By immediately marking failed speculative insertions as dead, other backends don't need to wait for the entire transaction to complete.

## Parameters / Member Variables
- `relation`: The heap relation containing the speculative tuple to abort
- `tid`: ItemPointer identifying the location of the speculative tuple to be killed

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionId
  - ItemPointerIsValid
  - ItemPointerGetBlockNumber
  - ReadBuffer
  - PageIsAllVisible
  - PageGetItemId
  - ItemIdIsNormal
  - HeapTupleHeaderIsSpeculative
  - IsToastRelation
  - HeapTupleHeaderIsHeapOnly
  - TransactionIdPrecedes
  - PageSetPrunable
  - HeapTupleHeaderSetXmin
  - compute_infobits
  - XLogBeginInsert
  - XLogRegisterData
  - XLogRegisterBuffer
  - XLogInsert
  - HeapTupleHasExternal
  - heap_toast_delete
  - ReleaseBuffer
  - pgstat_count_heap_delete
- Called from (representative examples):
  - toast_delete_datum
  - heapam_tuple_complete_speculative
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Uses WAL records identical to heap_delete() for recovery consistency
- Performs extensive validation to ensure the tuple is speculative and inserted by the current transaction
- Prevents unprincipled deadlocks by making failed speculative insertions immediately visible as dead
- Handles TOAST data cleanup for tuples with external storage
- Updates heap statistics by counting the deletion
- Sets pruning hints using TransactionXmin or relation's relfrozenxid for future cleanup efficiency
- Never requires catalog invalidation since catalogs don't support speculative insertion