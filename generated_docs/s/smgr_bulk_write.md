# smgr_bulk_write

## Location
[src/backend/storage/smgr/bulk_write.c:324-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/bulk_write.c#L324-L347)

## Overview
Queues a write operation for a given buffer as part of a bulk write operation, transferring ownership of the buffer to the bulk write state.

## Definition

```c
void
smgr_bulk_write(BulkWriteState *bulkstate, BlockNumber blocknum, BulkWriteBuffer buf, bool page_std)
```
## Detailed Description
This function is a core component of PostgreSQL's bulk write optimization system. It queues a write operation for a specific block without immediately performing the I/O operation. The function takes ownership of the provided buffer and stores the write request in a pending writes array within the bulk write state. When the pending writes array reaches its maximum capacity (MAX_PENDING_WRITES), it automatically triggers a flush operation to perform the actual I/O.

The bulk write mechanism is designed to optimize storage performance by batching multiple write operations together, reducing the overhead of individual I/O calls. This is particularly beneficial during operations like index builds, table rewrites, and other scenarios involving sequential writes to storage.

## Parameters / Member Variables
- : Pointer to the BulkWriteState structure that tracks the bulk write operation and contains the pending writes array
- : The block number where the buffer data should be written in the target relation
- : The BulkWriteBuffer containing the data to be written (ownership transfers to the bulk write state)
- : Boolean flag indicating whether the page follows the standard PostgreSQL page format

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_flush](smgr_bulk_flush.md)
  - MAX_PENDING_WRITES
- Called from (representative examples):
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md)
  - [end_heap_rewrite](../e/end_heap_rewrite.md)
  - [raw_heap_insert](../r/raw_heap_insert.md)
  - [_bt_blwritepage](../b/_bt_blwritepage.md)
  - [RelationCopyStorage](../R/RelationCopyStorage.md)

## Notes and Other Information
- **Ownership Transfer**: The function takes ownership of the provided buffer, meaning the caller should not use or free the buffer after this call
- **Write Uniqueness**: Each block can only be written once during a single bulk write operation
- **Automatic Flushing**: When the pending writes array is full, the function automatically calls smgr_bulk_flush() to perform the actual I/O
- **Performance Optimization**: This function is part of PostgreSQL's bulk write optimization strategy, commonly used during index builds and table maintenance operations
- **Memory Management**: The pending writes are stored in an array within the BulkWriteState structure, with automatic flushing preventing unbounded memory usage