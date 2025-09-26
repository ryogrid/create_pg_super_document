# table_index_delete_tuples

## Location
[src/include/access/tableam.h:1357-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1357-L1402)

## Overview
This function determines which index tuples are safe to delete based on their table TID by checking the vacuumability of corresponding table tuples and returns a snapshot conflict horizon for WAL logging.

## Definition
```c
static inline TransactionId
table_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
```

## Detailed Description
The `table_index_delete_tuples` function serves as a table access method abstraction for determining which index entries can be safely deleted during index cleanup operations. It examines the TM_IndexDeleteOp state provided by the index access method caller to identify which entries point to vacuumable table tuples.

The function delegates to the specific table access method implementation via `rel->rd_tableam->index_delete_tuples`. Entries found to be vacuumable are marked as deletable in the delstate structure, allowing the index access method to proceed with safe deletion.

A critical aspect of this function is its return of a snapshotConflictHorizon TransactionID. This transaction ID is embedded in the index deletion WAL record and is used during WAL replay in Hot Standby mode to determine if recovery conflicts are needed for the deletion operation.

## Parameters / Member Variables
- `rel`: The relation (table) whose tuples are being checked for vacuumability
- `delstate`: TM_IndexDeleteOp structure containing index entries to be evaluated for deletion, which gets modified to mark deletable entries

## Dependencies
- Functions called/Symbols referenced:
  - `rel->rd_tableam->index_delete_tuples` (access method-specific implementation)
  - `TM_IndexDeleteOp` (structure type)
- Called from (representative examples):
  - `index_compute_xid_horizon_for_tuples` (src/backend/access/index/genam.c:336)
  - `_bt_delitems_delete_check` (src/backend/access/nbtree/nbtpage.c:1526)

## Notes and Other Information
- Part of the table access method abstraction layer for supporting pluggable storage engines
- Critical for vacuum and index cleanup operations in PostgreSQL
- The returned TransactionID is essential for Hot Standby conflict resolution during WAL replay
- The function modifies the delstate parameter to mark which entries are safe to delete
- Used primarily by index access methods during bulk deletion operations
- The snapshotConflictHorizon helps maintain consistency in streaming replication scenarios