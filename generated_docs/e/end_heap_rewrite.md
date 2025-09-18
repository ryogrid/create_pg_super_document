# end_heap_rewrite

## Location
src/backend/access/heap/rewriteheap.c: 297 - 340

## Overview
Finalizes and cleans up a heap rewrite operation by processing any remaining unresolved tuples, flushing buffers, and freeing all associated resources.

## Definition


## Detailed Description
The `end_heap_rewrite` function completes a heap rewrite operation that was initiated with `begin_heap_rewrite`. It performs final cleanup tasks including processing any remaining unresolved tuples in the hash table, writing any buffered data to storage, and deallocating all memory contexts and resources associated with the rewrite operation.

The function iterates through any remaining entries in the unresolved tuples hash table and inserts them into the new heap relation. These remaining tuples are typically dead tuples, but the function errs on the side of safety by including them. It also ensures that any buffered page data is written to disk using the bulk write interface before completing the operation.

## Parameters / Member Variables
- `state`: The RewriteState structure containing all context and progress information for the rewrite operation, originally created by begin_heap_rewrite

## Dependencies
- Functions called/Symbols referenced:
  - hash_seq_init
  - hash_seq_search
  - ItemPointerSetInvalid
  - raw_heap_insert
  - smgr_bulk_write
  - smgr_bulk_finish
  - logical_end_heap_rewrite
  - MemoryContextDelete
- Called from (representative examples):
  - heapam_relation_copy_for_cluster

## Notes and Other Information
- Processes any remaining unresolved tuples by marking their CTIDs as invalid and inserting them via raw_heap_insert
- Flushes any remaining buffered page data to storage using the bulk write interface
- Integrates with logical replication by calling logical_end_heap_rewrite for proper change tracking cleanup
- Performs complete resource cleanup by deleting the entire memory context, which frees all subsidiary data structures
- Should always be called to properly complete a rewrite operation and prevent resource leaks
- Remaining unresolved tuples are typically dead but are inserted for safety