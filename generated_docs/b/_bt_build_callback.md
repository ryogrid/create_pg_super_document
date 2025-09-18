# _bt_build_callback

## Location
[src/backend/access/nbtree/nbtsort.c:577-605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L577-L605)

## Overview
A per-tuple callback function used during B-tree index building that processes each tuple scanned from the heap table and directs it to the appropriate spool for subsequent processing.

## Definition


## Detailed Description
This function serves as the callback mechanism for table_index_build_scan during B-tree index construction. It receives each tuple from the heap scan and determines how to handle it based on its visibility status. Live tuples are directed to the primary spool, while dead tuples (when MVCC snapshot isolation is used) are directed to a secondary spool for separate handling. This separation allows the index build process to handle different tuple visibility states appropriately during concurrent operations.

## Parameters / Member Variables
- `index`: The B-tree index relation being built
- `tid`: Item pointer (TID) of the tuple in the heap table
- `values`: Array of Datum values for the index columns
- `isnull`: Array of boolean flags indicating null values for each column
- `tupleIsAlive`: Boolean flag indicating whether the tuple is visible/alive
- `state`: Void pointer to BTBuildState structure containing build context

## Dependencies
- Functions called/Symbols referenced:
  - [BTBuildState](../B/BTBuildState.md) (cast from state parameter)
  - [_bt_spool](_bt_spool.md) (called to insert tuples into spools)
  - BulkWriteBuffer (referenced in context)
- Called from (representative examples):
  - [_bt_spools_heapscan](_bt_spools_heapscan.md)
  - [_bt_parallel_scan_and_sort](_bt_parallel_scan_and_sort.md)

## Notes and Other Information
- This is a static function, only accessible within the nbtsort.c compilation unit
- The function maintains a count of processed tuples in buildstate->indtuples
- Dead tuples are only handled separately when spool2 exists (buildstate->spool2 != NULL)
- Sets the havedead flag when dead tuples are encountered, which affects subsequent processing phases
- Part of the PostgreSQL B-tree index building infrastructure that supports concurrent index creation