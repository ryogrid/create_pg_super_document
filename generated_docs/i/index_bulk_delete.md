# index_bulk_delete

## Location
src/backend/access/index/indexam.c: 748 - 768

## Overview
Performs mass deletion of index entries by using a callback routine to determine which main-heap tuples should be deleted from the index.

## Definition
```c
IndexBulkDeleteResult *index_bulk_delete(IndexVacuumInfo *info,
                                         IndexBulkDeleteResult *istat,
                                         IndexBulkDeleteCallback callback,
                                         void *callback_state)
```

## Detailed Description
The `index_bulk_delete` function is a critical component of PostgreSQL's VACUUM operation, responsible for efficiently removing multiple index entries in a single operation. Rather than deleting entries one by one, this function uses a callback-driven approach where the caller provides a function that determines whether a given heap tuple should be deleted from the index.

This bulk deletion mechanism is essential for maintaining index consistency during VACUUM operations, where dead tuples identified in the heap need to be removed from all associated indexes. The function delegates the actual deletion work to the access method's specific `ambulkdelete` procedure, allowing different index types to implement their own optimized bulk deletion strategies.

The function can accumulate statistics across multiple calls by accepting and returning an `IndexBulkDeleteResult` structure, enabling efficient tracking of deletion operations across large tables.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing index vacuum context, including the index relation and vacuum parameters
- `istat`: IndexBulkDeleteResult structure with accumulated statistics from previous bulk delete operations (can be NULL for first call)  
- `callback`: IndexBulkDeleteCallback function pointer that determines whether a given tuple should be deleted
- `callback_state`: Opaque state data passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (macro for relation validation)
  - CHECK_REL_PROCEDURE (macro to verify ambulkdelete procedure exists)
  - ambulkdelete (access method specific bulk deletion procedure)
- Called from (representative examples):
  - validate_index (index validation during creation)
  - vac_bulkdel_one_index (vacuum bulk deletion for single index)
  - IndexScanIsValid (index scan validation)

## Notes and Other Information
- Returns an optional palloc'd IndexBulkDeleteResult structure containing deletion statistics
- The callback mechanism allows flexible deletion criteria without requiring the index AM to understand heap-specific logic
- Essential for VACUUM performance as it avoids the overhead of individual tuple deletions
- Different index types (B-tree, GiST, GIN, etc.) can optimize bulk deletion based on their internal structure
- The statistics accumulation feature allows tracking deletion progress across multiple passes over large indexes