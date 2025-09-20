# end_heap_rewrite

## Location
[src/backend/access/heap/rewriteheap.c:297-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L297-L340)

## Overview
Finalizes and cleans up a heap rewrite operation by processing any remaining unresolved tuples, flushing buffers, and freeing all associated resources.

## Definition

```c
void
end_heap_rewrite(RewriteState state)
```
## Detailed Description
The `end_heap_rewrite` function completes a heap rewrite operation that was initiated with `begin_heap_rewrite`. It performs final cleanup tasks including processing any remaining unresolved tuples in the hash table, writing any buffered data to storage, and deallocating all memory contexts and resources associated with the rewrite operation.

The function iterates through any remaining entries in the unresolved tuples hash table and inserts them into the new heap relation. These remaining tuples are typically dead tuples, but the function errs on the side of safety by including them. It also ensures that any buffered page data is written to disk using the bulk write interface before completing the operation.

## Parameters / Member Variables
- `state`: The RewriteState structure containing all context and progress information for the rewrite operation, originally created by begin_heap_rewrite

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [raw_heap_insert](../r/raw_heap_insert.md)
  - [smgr_bulk_write](../s/smgr_bulk_write.md)
  - [smgr_bulk_finish](../s/smgr_bulk_finish.md)
  - [logical_end_heap_rewrite](../l/logical_end_heap_rewrite.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)

## Notes and Other Information
- Processes any remaining unresolved tuples by marking their CTIDs as invalid and inserting them via raw_heap_insert
- Flushes any remaining buffered page data to storage using the bulk write interface
- Integrates with logical replication by calling logical_end_heap_rewrite for proper change tracking cleanup
- Performs complete resource cleanup by deleting the entire memory context, which frees all subsidiary data structures
- Should always be called to properly complete a rewrite operation and prevent resource leaks
- Remaining unresolved tuples are typically dead but are inserted for safety