# lazy_cleanup_one_index

## Location
src/backend/access/heap/vacuumlazy.c: 2470 - 2529

## Overview
Performs post-vacuum cleanup operations for a single index relation by calling the index access method's amvacuumcleanup routine.

## Definition
```c
static IndexBulkDeleteResult *
lazy_cleanup_one_index(Relation indrel, IndexBulkDeleteResult *istat,
                       double reltuples, bool estimated_count,
                       LVRelState *vacrel)
```

## Detailed Description
This function handles the cleanup phase of vacuum operations for individual indexes. It sets up the necessary vacuum information structure (IndexVacuumInfo) with parameters needed by the index access method's cleanup routine. The function manages error tracking by updating vacuum error information to include the current index name, then calls the generic index cleanup function vac_cleanup_one_index. After cleanup completion, it restores the previous error tracking state and frees allocated memory for the index name.

## Parameters / Member Variables
- `indrel`: The index relation to be cleaned up
- `istat`: Input bulk delete statistics from previous vacuum operations
- `reltuples`: Number of heap tuples (actual or estimated)
- `estimated_count`: Boolean indicating whether reltuples is an estimated value
- `vacrel`: Vacuum relation state containing vacuum context and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md)
  - [LVSavedErrInfo](../L/LVSavedErrInfo.md)
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)
  - [vac_cleanup_one_index](../v/vac_cleanup_one_index.md)
  - [restore_vacuum_error_info](../r/restore_vacuum_error_info.md)
  - VACUUM_ERRCB_PHASE_INDEX_CLEANUP
- Called from (representative examples):
  - [lazy_cleanup_all_indexes](lazy_cleanup_all_indexes.md)

## Notes and Other Information
The function is part of the lazy vacuum implementation and specifically handles the cleanup phase after bulk deletion operations. It ensures proper error reporting by temporarily updating the vacuum error context with the current index name. The estimated_count parameter is passed through to the index access method to inform it whether tuple count statistics are precise or estimated, which may affect optimization decisions during cleanup.