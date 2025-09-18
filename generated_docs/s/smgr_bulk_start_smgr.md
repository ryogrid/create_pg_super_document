# smgr_bulk_start_smgr

## Location
src/backend/storage/smgr/bulk_write.c: 101 - 130

## Overview
Initializes and returns a BulkWriteState for performing bulk write operations on a relation fork using the storage manager interface directly.

## Definition
```c
BulkWriteState *smgr_bulk_start_smgr(SMgrRelation smgr, ForkNumber forknum, bool use_wal)
```

## Detailed Description
This function creates and initializes a BulkWriteState structure for bulk write operations. Unlike smgr_bulk_start_rel, it works directly with an SMgrRelation and doesn't require a relcache entry, making it more flexible for scenarios where only the storage manager is available. The function sets up the initial state including the current relation size, WAL logging preferences, and captures the current redo pointer for crash recovery consistency.

## Parameters / Member Variables
- `smgr`: The storage manager relation object representing the relation to write to
- `forknum`: The fork number specifying which fork of the relation to write to
- `use_wal`: Boolean flag indicating whether WAL logging should be used for this bulk operation

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - smgrnblocks
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - SMgrRelation
  - [BulkWriteState](../B/BulkWriteState.md)
  - CurrentMemoryContext
- Called from (representative examples):
  - [RelationCopyStorage](../R/RelationCopyStorage.md)
  - [smgr_bulk_start_rel](smgr_bulk_start_rel.md)

## Notes and Other Information
- This is the lower-level interface compared to smgr_bulk_start_rel, providing more direct control over bulk write parameters
- The function captures the current redo pointer at initialization time for consistency checking during crash recovery
- Memory context is saved to ensure all subsequent buffer allocations use the same context
- The relation size is determined at initialization time by calling smgrnblocks
- The npending counter is initialized to 0, tracking the number of pending write operations