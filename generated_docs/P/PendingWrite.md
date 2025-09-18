# PendingWrite

## Location
src/backend/storage/smgr/bulk_write.c: 52 - 57

## Overview
PendingWrite is a struct that represents a single pending write operation in PostgreSQL's bulk write system, containing the buffer data, block number, and page layout information for efficient batch processing of writes.

## Definition


## Detailed Description
The PendingWrite struct is a core component of PostgreSQL's bulk write optimization system located in . It represents a single write operation that has been queued but not yet flushed to disk. The bulk write system accumulates multiple PendingWrite entries in an array within the BulkWriteState structure, allowing PostgreSQL to batch multiple write operations together for improved I/O efficiency.

When writes are queued using , they are stored as PendingWrite entries until either the maximum number of pending writes (, defined as ) is reached or the bulk write operation is explicitly flushed. During flush operations, the pending writes are sorted by block number to optimize disk access patterns before being written to storage.

The struct plays a crucial role in WAL (Write-Ahead Logging) optimization by allowing multiple pages to be logged together in a single WAL record, reducing the overhead of individual write operations.

## Parameters / Member Variables
- : A BulkWriteBuffer (typedef for PGIOAlignedBlock*) that contains the actual page data to be written to disk
- : The BlockNumber (block number) indicating the target location where this page should be written within the relation fork
- : A boolean flag indicating whether the page uses standard PostgreSQL page layout (true) or a custom/non-standard layout (false)

## Dependencies
- Functions called/Symbols referenced:
  - BulkWriteBuffer (typedef for the buffer type)
  - BlockNumber (for block addressing)

- Called from (representative examples):
  - BulkWriteState (contains an array of PendingWrite structs)
  - buffer_cmp (comparison function for sorting pending writes)
  - smgr_bulk_flush (processes pending writes for actual disk I/O)
  - smgr_bulk_write (creates and queues new PendingWrite entries)

## Notes and Other Information
- The PendingWrite struct is always used as part of an array within BulkWriteState, never as standalone instances
- The maximum number of pending writes is limited by MAX_PENDING_WRITES constant
- During flush operations, PendingWrite entries are sorted by block number using the buffer_cmp comparison function to optimize sequential disk access
- The page_std flag affects WAL logging behavior - if any page in a batch uses non-standard layout, all pages in that batch are logged as non-standard for consistency
- Each PendingWrite represents ownership transfer of the buffer - once queued, the bulk write system owns the buffer memory
- Duplicate block numbers are not allowed within the same bulk write operation, as enforced by assertions in the comparison function