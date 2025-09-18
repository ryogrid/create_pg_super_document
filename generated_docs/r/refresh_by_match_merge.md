# refresh_by_match_merge

## Location
src/backend/commands/matview.c: 597 - 887

## Overview
Refreshes a materialized view with transactional semantics while allowing concurrent reads by performing a diff-based merge using a full outer join between the old and new data versions.

## Definition


## Detailed Description
This function implements a sophisticated materialized view refresh strategy that allows concurrent reads during the refresh operation. It works by:

1. **Creating a temporary diff table**: Performs a full outer join between the existing materialized view data and the new data (stored in a temporary table) to identify differences
2. **Duplicate detection**: Validates that the new data contains no duplicate rows without NULLs, which is essential for the diff algorithm to work correctly
3. **Unique index requirement**: Requires at least one usable unique index on the materialized view to ensure proper row identification and matching
4. **Set-based operations**: Uses efficient DELETE and INSERT operations based on the diff results rather than row-by-row processing
5. **Transactional safety**: Maintains ACID properties while allowing concurrent SELECT operations

The function leverages the behavior of NULLs in equality tests and UNIQUE indexes to correctly handle rows with NULL values. The entire operation is performed under an ExclusiveLock to prevent concurrent REFRESH operations and incremental maintenance.

## Parameters / Member Variables
- : Object ID of the materialized view to refresh
- : Object ID of the temporary table containing the new data
- : User ID of the relation owner for security context switching
- : Saved security context for restoration after temporary privilege changes

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close
  - SPI_connect, SPI_exec, SPI_execute, SPI_finish
  - [RelationGetIndexList](../R/RelationGetIndexList.md), index_open, index_close
  - [is_usable_unique_index](../i/is_usable_unique_index.md)
  - [OpenMatViewIncrementalMaintenance](../O/OpenMatViewIncrementalMaintenance.md), CloseMatViewIncrementalMaintenance
  - quote_qualified_identifier, generate_operator_clause
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)

## Notes and Other Information
- Requires at least one usable unique index on the materialized view to function correctly
- Cannot handle duplicate rows without NULLs in the new data set
- Uses the Server Programming Interface (SPI) extensively for SQL operations
- Temporarily switches security context to create temporary tables outside of SECURITY_RESTRICTED_OPERATION mode
- The diff table contains both the TID of matched old records and the complete new row data as a composite type
- Performs deletes before inserts to maintain referential integrity