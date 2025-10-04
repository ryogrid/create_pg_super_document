# vac_bulkdel_one_index

## Location
[src/backend/commands/vacuum.c:2537-2557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L2537-L2557)

## Overview
Performs bulk deletion operation on a single index relation, removing dead tuple references identified during vacuum operations.

## Definition

```c
IndexBulkDeleteResult *
vac_bulkdel_one_index(IndexVacuumInfo *ivinfo, IndexBulkDeleteResult *istat,
					  TidStore *dead_items, VacDeadItemsInfo *dead_items_info)
```
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
  - [index_bulk_delete](../i/index_bulk_delete.md)
  - [vac_tid_reaped](vac_tid_reaped.md)
  - ereport
  - [errmsg](../e/errmsg.md)
  - RelationGetRelationName
- Called from (representative examples):
  - [lazy_vacuum_one_index](../l/lazy_vacuum_one_index.md)
  - [parallel_vacuum_process_one_index](../p/parallel_vacuum_process_one_index.md)

## Notes and Other Information
- Reports operation progress using ereport() with configurable message level
- Uses vac_tid_reaped as the callback function for determining which tuples to delete
- The dead_items TidStore parameter is passed as void* to index_bulk_delete callback
- Returns updated statistics that accumulate results across multiple bulk delete operations
- Part of the vacuum infrastructure for maintaining index consistency during heap cleanup
- Log messages include index name and count of removed row versions for monitoring purposes

## Simplified Source

```c
IndexBulkDeleteResult *
vac_bulkdel_one_index(IndexVacuumInfo *ivinfo, IndexBulkDeleteResult *istat,
                      TidStore *dead_items, VacDeadItemsInfo *dead_items_info)
{
    // Perform bulk deletion using callback to check if TIDs are dead
    istat = index_bulk_delete(ivinfo, istat, vac_tid_reaped, (void *) dead_items);

    // Report progress
    ereport(ivinfo->message_level,
            (errmsg("scanned index \"%s\" to remove %lld row versions",
                    RelationGetRelationName(ivinfo->index),
                    (long long) dead_items_info->num_items)));

    return istat;
}
```