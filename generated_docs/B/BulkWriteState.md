# BulkWriteState

## Location
src/backend/storage/smgr/bulk_write.c: 62 - 87

## Overview
BulkWriteState is a structure that maintains state for bulk write operations on a single relation fork, allowing efficient batch processing of page writes with WAL logging optimization.

## Definition


## Detailed Description
BulkWriteState is a core data structure in PostgreSQL's bulk write optimization system, designed to efficiently handle large-scale write operations to relation files. The structure batches multiple pending writes together before committing them to storage and WAL, which significantly improves performance during bulk operations like index builds, table rewrites, and large data loads.

The structure maintains a queue of pending writes that are processed in batches, allowing the system to optimize both disk I/O and WAL logging by reducing the number of system calls and improving sequential access patterns. This is particularly beneficial for operations that write many pages to a relation in sequence.

## Parameters / Member Variables
- : Storage manager relation handle for the target relation being written to
- : Fork number identifying which fork of the relation (main, FSM, visibility map, etc.) is being written
- : Boolean flag indicating whether WAL logging should be used for this bulk operation
- : Count of currently queued pending writes awaiting flush
- : Array of PendingWrite structures holding the queued write operations (maximum of MAX_PENDING_WRITES entries)
- : Current size of the relation in blocks, maintained to track relation growth
- : WAL record pointer captured at the beginning of the bulk operation, used for crash recovery consistency
- : Memory context used for allocations related to this bulk write operation

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation
  - PendingWrite
  - MAX_PENDING_WRITES
  - smgr_bulk_flush

- Called from (representative examples):
  - smgr_bulk_start_rel
  - smgr_bulk_start_smgr
  - smgr_bulk_finish
  - smgr_bulk_write
  - smgr_bulk_get_buf
  - Various index building functions (btbuildempty, spgbuildempty)
  - Table rewrite operations (RewriteStateData, RelationCopyStorage)

## Notes and Other Information
- The structure is defined in src/backend/storage/smgr/bulk_write.c:62-87
- MAX_PENDING_WRITES is defined as XLR_MAX_BLOCK_ID, limiting the number of writes that can be batched
- This optimization is particularly important for index builds and table reorganization operations where many pages are written sequentially
- The start_RedoRecPtr is crucial for ensuring crash recovery consistency by tracking the WAL position when bulk operations begin
- The use_wal flag allows certain operations to bypass WAL logging when it's safe to do so (e.g., during index builds that can be recreated)