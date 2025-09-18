# ATExecClusterOn

## Location
src/backend/commands/tablecmds.c: 14851 - 14882

## Overview
Implements the ALTER TABLE CLUSTER ON command by setting the specified index as the clustering index for the table, updating the indisclustered bits in the system catalog.

## Definition
```c
static ObjectAddress ATExecClusterOn(Relation rel, const char *indexName, LOCKMODE lockmode)
```

## Detailed Description
This function executes the ALTER TABLE CLUSTER ON operation, which designates a specific index as the clustering index for a table. Clustering means that the table's physical storage is organized according to the order of the specified index, which can improve performance for queries that use that index. The function performs validation to ensure the specified index exists and is suitable for clustering, then updates the system catalog to mark it as the clustering index.

The function validates that the index exists in the same namespace as the table, verifies it is suitable for clustering using check_index_is_clusterable, and then calls mark_index_clustered to update the pg_index system catalog. The actual physical reordering of table data is handled by the CLUSTER command separately.

## Parameters / Member Variables
- `rel`: The relation (table) on which to set the clustering index
- `indexName`: The name of the index to be designated as the clustering index
- `lockmode`: The lock mode to use when accessing the index relation

## Dependencies
- Functions called/Symbols referenced:
  - [get_relname_relid](../g/get_relname_relid.md)
  - RelationGetRelationName
  - [check_index_is_clusterable](../c/check_index_is_clusterable.md)
  - [mark_index_clustered](../m/mark_index_clustered.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- This is a static function only accessible within tablecmds.c as part of the ALTER TABLE infrastructure
- Returns an ObjectAddress pointing to the newly designated clustering index
- The function only updates catalog metadata; actual table reordering requires a separate CLUSTER command
- Validates index existence and clustering suitability before making changes
- Part of the ALTER TABLE command execution framework in PostgreSQL
- Located in src/backend/commands/tablecmds.c:14851-14882