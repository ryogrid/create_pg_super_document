# vac_bulkdel_one_index

## Location
src/backend/commands/vacuum.c: 2537 - 2557

## Overview
Performs bulk deletion operation on a single index relation, removing dead tuple references identified during vacuum operations.

## Definition


## Detailed Description
This function executes bulk deletion on a single index relation as part of the vacuum process. It delegates the actual deletion work to index_bulk_delete(), using vac_tid_reaped as the callback function to determine which tuples should be deleted based on the provided TidStore. After completion, it reports the operation results including the number of row versions removed from the index. The function maintains and returns updated bulk delete statistics that can be used for subsequent operations or reporting.

## Parameters / Member Variables
- : IndexVacuumInfo structure containing vacuum parameters and index relation information
- : Current IndexBulkDeleteResult statistics (can be NULL for first call)
- : TidStore containing the set of dead tuple identifiers to be removed
- : VacDeadItemsInfo structure with metadata about dead items including count
- Returns: Updated IndexBulkDeleteResult with bulk deletion statistics

## Dependencies
- Functions called/Symbols referenced:
  - index_bulk_delete
  - vac_tid_reaped
  - ereport
  - errmsg
  - RelationGetRelationName
- Called from (representative examples):
  - lazy_vacuum_one_index
  - parallel_vacuum_process_one_index

## Notes and Other Information
- Reports operation progress using ereport() with configurable message level
- Uses vac_tid_reaped as the callback function for determining which tuples to delete
- The dead_items TidStore parameter is passed as void* to index_bulk_delete callback
- Returns updated statistics that accumulate results across multiple bulk delete operations
- Part of the vacuum infrastructure for maintaining index consistency during heap cleanup
- Log messages include index name and count of removed row versions for monitoring purposes