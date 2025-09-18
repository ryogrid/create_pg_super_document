# CopyStatistics

## Location
src/backend/catalog/heap.c: 2921 - 2973

## Overview
Copies statistical information entries from pg_statistic catalog from one relation to another, preserving column statistics during relation operations such as concurrent index rebuilds.

## Definition
void CopyStatistics(Oid fromrelid, Oid torelid)

## Detailed Description
This function duplicates statistical information stored in the pg_statistic system catalog from a source relation to a destination relation. It performs a comprehensive transfer of all statistics entries, which include information about column value distributions, most common values, histograms, and other data that the query planner uses for cost estimation.

The function operates by:
1. Opening the pg_statistic catalog with exclusive row-level locking
2. Scanning for all statistics entries belonging to the source relation
3. Creating modifiable copies of each statistics tuple
4. Updating the relation ID to point to the destination relation
5. Inserting the modified tuples into the catalog with proper index updates
6. Cleaning up resources and releasing locks

This operation is critical during certain DDL operations where a relation is rebuilt or recreated, ensuring that query planning performance is maintained by preserving the statistical information.

## Parameters / Member Variables
- `fromrelid`: OID of the source relation whose statistics should be copied
- `torelid`: OID of the destination relation that will receive the copied statistics

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_statistic catalog relation
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for searching by relation ID
  - [systable_beginscan](../s/systable_beginscan.md): Begins system catalog scan with index
  - [systable_getnext](../s/systable_getnext.md): Retrieves next tuple from the scan
  - [heap_copytuple](../h/heap_copytuple.md): Creates a modifiable copy of a heap tuple
  - [CatalogOpenIndexes](CatalogOpenIndexes.md): Opens indexes for efficient catalog updates
  - [CatalogTupleInsertWithInfo](CatalogTupleInsertWithInfo.md): Inserts tuple with index information
  - [heap_freetuple](../h/heap_freetuple.md): Frees memory allocated for a heap tuple
  - [CatalogCloseIndexes](CatalogCloseIndexes.md): Closes catalog indexes
  - [systable_endscan](../s/systable_endscan.md): Ends the system catalog scan
  - table_close: Closes the catalog relation

- Called from (representative examples):
  - index_concurrently_swap: During concurrent index rebuild operations

## Notes and Other Information
- The function uses RowExclusiveLock to ensure exclusive access during statistics modification
- Index state is opened lazily only when needed (when the first tuple is found)
- All copied tuples have their relation ID updated to point to the destination relation
- The function preserves all other statistical information unchanged
- This is part of PostgreSQL's statistics management system that supports the query planner
- The function is commonly used during operations that rebuild relations while preserving optimizer information
- Proper cleanup ensures that heap tuples are freed to prevent memory leaks