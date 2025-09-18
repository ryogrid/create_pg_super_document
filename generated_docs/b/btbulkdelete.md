# btbulkdelete

## Location
src/backend/access/nbtree/nbtree.c: 821 - 850

## Overview
Performs bulk deletion of B-tree index entries pointing to a set of heap tuples specified by a callback routine.

## Definition
```c
IndexBulkDeleteResult *btbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                   IndexBulkDeleteCallback callback, void *callback_state)
```

## Detailed Description
This function is the main entry point for bulk deletion operations in B-tree indexes. It coordinates the deletion of multiple index entries at once, which is more efficient than deleting entries individually. The function determines which tuples to delete through a callback mechanism that identifies target heap tuples by their ItemPointer.

The operation follows these steps:
1. Allocates or reuses a statistics structure to track the operation
2. Establishes a vacuum cycle ID for coordination and cleanup
3. Calls btvacuumscan to perform the actual scanning and deletion
4. Ensures proper cleanup even in case of errors using PostgreSQL's error handling macros
5. Returns statistical information for VACUUM display purposes

The function uses PostgreSQL's PG_ENSURE_ERROR_CLEANUP mechanism to guarantee that shared memory cleanup occurs even if an error is thrown during the vacuum operation.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing information about the vacuum operation and the index being processed
- `stats`: IndexBulkDeleteResult structure for tracking statistics (can be NULL for first-time allocation)
- `callback`: Function pointer to IndexBulkDeleteCallback that determines which tuples should be deleted
- `callback_state`: Opaque state data passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - palloc0
  - _bt_start_vacuum
  - btvacuumscan
  - _bt_end_vacuum
  - _bt_end_vacuum_callback
- Macros used:
  - PG_ENSURE_ERROR_CLEANUP
  - PG_END_ENSURE_ERROR_CLEANUP
  - PointerGetDatum
- Types used:
  - IndexVacuumInfo
  - IndexBulkDeleteResult
  - IndexBulkDeleteCallback
  - BTCycleId
  - Relation
- Called from:
  - bthandler

## Notes and Other Information
- Returns a palloc'd struct containing statistical information for VACUUM displays
- The function can be called multiple times during a single VACUUM operation, reusing the stats structure
- Uses a vacuum cycle ID to coordinate with concurrent operations and ensure consistency
- The error cleanup mechanism ensures that shared memory resources are properly released even if the operation fails
- This is part of the standard PostgreSQL access method interface for bulk delete operations
- The actual deletion work is delegated to btvacuumscan, making this function primarily a coordinator and error handler