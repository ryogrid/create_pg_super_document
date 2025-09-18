# smgr_bulk_start_rel

## Location
[src/backend/storage/smgr/bulk_write.c:88-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/bulk_write.c#L88-L100)

## Overview
A convenience function that starts a bulk write operation on a relation fork by wrapping the underlying smgr_bulk_start_smgr function with relation-specific parameters.

## Definition
```c
BulkWriteState *smgr_bulk_start_rel(Relation rel, ForkNumber forknum)
```

## Detailed Description
This function serves as a relation-oriented wrapper around the more general smgr_bulk_start_smgr function. It automatically extracts the storage manager from the given relation and determines whether WAL logging is required based on the relation's properties. The function is designed to simplify bulk write operations by handling the common case where you have a Relation object and want to perform bulk writes on one of its forks.

## Parameters / Member Variables
- `rel`: The relation on which to perform bulk write operations
- `forknum`: The fork number specifying which fork of the relation to write to (main, fsm, vm, or init)

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_start_smgr](smgr_bulk_start_smgr.md)
  - RelationGetSmgr
  - RelationNeedsWAL
  - INIT_FORKNUM
  - [BulkWriteState](../B/BulkWriteState.md)
- Called from (representative examples):
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md)
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md)
  - [btbuildempty](../b/btbuildempty.md)
  - _bt_load
  - [spgbuildempty](spgbuildempty.md)

## Notes and Other Information
- This function automatically determines WAL logging requirements by checking if the relation needs WAL logging or if the operation is on the INIT_FORKNUM
- The INIT_FORKNUM always requires WAL logging regardless of the relation's WAL requirements
- This is a higher-level interface compared to smgr_bulk_start_smgr, making it more convenient for operations that work with Relation objects