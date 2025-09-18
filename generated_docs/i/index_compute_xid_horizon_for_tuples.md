# index_compute_xid_horizon_for_tuples

## Location
[src/backend/access/index/genam.c:293-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L293-L385)

## Overview
Computes a snapshot conflict horizon for index tuples being deleted by consulting the table access method to determine the appropriate transaction ID for WAL records.

## Definition


## Detailed Description
index_compute_xid_horizon_for_tuples is a generic helper function that provides index access methods with a standardized way to obtain snapshot conflict horizon values when deleting index tuples. The function serves as a shim around table_index_delete_tuples(), providing the table access method with information about index tuples to be deleted and receiving back the appropriate snapshotConflictHorizon value for use in deletion WAL records.

The function operates on index tuples that are already known to be deletable (typically marked with LP_DEAD line pointer status) and builds the necessary data structures to communicate with the table access method. It assumes the standard IndexTuple representation where table TIDs are stored in the t_tid field and validates that all specified line pointers are properly marked as dead.

## Parameters / Member Variables
- : The index relation containing the tuples to be deleted
- : The heap (table) relation that the index points to
- : Buffer containing the index page with tuples to be deleted
- : Array of offset numbers identifying the index tuples to be deleted
- : Number of items in the itemnos array

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (get block number from buffer)
  - [PageGetItemId](../P/PageGetItemId.md) (get item ID from page)
  - [PageGetItem](../P/PageGetItem.md) (get item from page)
  - ItemIdIsDead (check if line pointer is marked dead)
  - [ItemPointerCopy](../I/ItemPointerCopy.md) (copy item pointer)
  - table_index_delete_tuples (main table AM interface)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [gistprunepage](../g/gistprunepage.md) (GiST index page pruning)
  - [_hash_vacuum_one_page](../h/_hash_vacuum_one_page.md) (Hash index page cleanup)

## Notes and Other Information
- All line pointers for the specified offset numbers must already be marked LP_DEAD
- The function assumes standard IndexTuple representation with TIDs in the t_tid field
- Returns InvalidTransactionId if no valid horizon can be determined
- Used primarily by index AMs that need snapshot conflict horizons for their deletion WAL records
- The function can be safely skipped by index AMs that don't require snapshot conflict horizon values
- Memory for temporary data structures (deltids and status arrays) is allocated and freed within the function
- The table access method is expected to confirm that all items are indeed deletable