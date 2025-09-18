# make_new_heap

## Location
src/backend/commands/cluster.c: 688 - 813

## Overview
Creates a transient table with the same logical structure as an existing table but with specified physical storage properties, used during CLUSTER, ALTER TABLE, and similar operations.

## Definition


## Detailed Description
The `make_new_heap` function creates a temporary table that duplicates the logical structure (columns, data types) of an existing table while allowing different physical storage characteristics. This is a critical component of PostgreSQL's table reorganization operations.

Key aspects of the function:
1. Creates a new heap table with a temporary name ("pg_temp_" + original OID)
2. Preserves the original table's column structure and data types
3. Copies reloptions from the original table to maintain storage parameters
4. Creates an associated TOAST table if the original had one
5. Does not copy constraints, defaults, or indexes (these are rebuilt later)

The function handles both regular and temporary tables appropriately, placing temporary tables in the pg_temp namespace and preserving the mapped relation status when necessary.

## Parameters / Member Variables
- `OIDOldHeap`: OID of the original table whose structure should be duplicated
- `NewTableSpace`: OID of the tablespace where the new table should be created
- `NewAccessMethod`: OID of the access method (table AM) to use for the new table
- `relpersistence`: Persistence characteristic (permanent, temporary, unlogged)
- `lockmode`: Lock mode to acquire on the original table

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the original relation
  - RelationGetDescr: Gets the table's tuple descriptor
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up relation information in system cache
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md): Retrieves relation options from cache
  - [LookupCreationNamespace](../L/LookupCreationNamespace.md): Finds appropriate namespace for temporary tables
  - RelationGetNamespace: Gets the namespace of the original relation
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md): Creates the new table with catalog entries
  - CommandCounterIncrement: Makes new catalog entries visible
  - [NewHeapCreateToastTable](../N/NewHeapCreateToastTable.md): Creates TOAST table if needed
  - table_close: Closes the original relation
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md): Part of clustering operation
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md): Materialized view refresh
  - [ATRewriteTables](../A/ATRewriteTables.md): Table rewriting during ALTER TABLE

## Notes and Other Information
- The new table does not inherit constraints, defaults, or indexes from the original
- Uses a naming convention of "pg_temp_" + original table OID to avoid conflicts
- Preserves storage options (reloptions) from the original table
- Handles TOAST tables appropriately, creating new TOAST relations when needed
- The mapped relation property is preserved for system catalogs like pg_class
- Returns the OID of the newly created table for use in subsequent operations
- Critical for maintaining data consistency during table reorganization operations