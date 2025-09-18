# lazy_vacuum_one_index

## Location
[src/backend/access/heap/vacuumlazy.c:2421-2469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2421-L2469)

## Overview
Removes dead index tuples from a single index by performing bulk deletion of entries corresponding to dead heap tuples, delegating the actual work to the index access method's ambulkdelete routine.

## Definition


## Detailed Description
This function performs the actual index vacuuming work for a single index during the vacuum process. It prepares an IndexVacuumInfo structure with the necessary parameters and then calls the generic vac_bulkdel_one_index function to perform the bulk deletion operation. The function handles error tracking by updating the vacuum error callback information to include the index name being processed.

The bulk deletion process removes all index tuples that point to dead heap tuples collected in vacrel->dead_items. The exact mechanism depends on the specific index access method's ambulkdelete routine implementation. The function assumes the reltuples count is estimated, which affects how the index access method processes the statistics.

The function carefully manages error context information, temporarily storing the index name in the vacuum state for error reporting purposes and cleaning it up when done.

## Parameters / Member Variables
- : The index relation being vacuumed
- : Previous IndexBulkDeleteResult from earlier operations, or NULL for the first call
- : Estimated number of heap tuples to be passed to the index AM's bulkdelete callback  
- : LVRelState containing vacuum state including dead_items collection and buffer access strategy

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - RelationGetRelationName
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)
  - [vac_bulkdel_one_index](../v/vac_bulkdel_one_index.md)
  - [restore_vacuum_error_info](../r/restore_vacuum_error_info.md)
  - [pfree](../p/pfree.md)
- Called from:
  - [lazy_vacuum_all_indexes](lazy_vacuum_all_indexes.md)

## Notes and Other Information
- Returns updated IndexBulkDeleteResult structure containing statistics from the bulk deletion
- Sets analyze_only to false since this is actual vacuuming, not analysis
- Sets report_progress to false as progress is tracked at a higher level
- Uses DEBUG2 message level for index AM operations
- Passes estimated_count as true, indicating reltuples is an estimate
- Temporarily stores index name in vacrel->indname for error reporting context
- Updates vacuum error phase to VACUUM_ERRCB_PHASE_VACUUM_INDEX during execution
- The actual deletion logic is delegated to the index access method via vac_bulkdel_one_index
- Uses the same buffer access strategy as the heap vacuum operations