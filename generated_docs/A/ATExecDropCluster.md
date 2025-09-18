# ATExecDropCluster

## Location
src/backend/commands/tablecmds.c: 14883 - 14894

## Overview
Implements the ALTER TABLE SET WITHOUT CLUSTER command by removing the clustering designation from any index on the table.

## Definition
```c
static void ATExecDropCluster(Relation rel, LOCKMODE lockmode)
```

## Detailed Description
This function executes the ALTER TABLE SET WITHOUT CLUSTER operation, which removes the clustering designation from a table. When a table has a clustering index, the indisclustered bit is set in the pg_index system catalog to indicate which index defines the table's clustering order. This function clears that designation by calling mark_index_clustered with InvalidOid, which turns off the indisclustered bit for all indexes on the table.

The operation is straightforward - it simply delegates to mark_index_clustered with InvalidOid to indicate that no index should be marked as clustered. This effectively removes any clustering designation from the table without affecting the physical storage or the indexes themselves.

## Parameters / Member Variables
- `rel`: The relation (table) from which to remove clustering designation
- `lockmode`: The lock mode to use (parameter is passed through but not directly used in this simple function)

## Dependencies
- Functions called/Symbols referenced:
  - mark_index_clustered
- Called from (representative examples):
  - ATExecCmd

## Notes and Other Information
- This is a static function only accessible within tablecmds.c as part of the ALTER TABLE infrastructure
- The function is very simple, consisting of just one call to mark_index_clustered
- Does not return a value as there is no specific object address to return
- Removes clustering designation without affecting the physical table storage or index structures
- Part of the ALTER TABLE command execution framework in PostgreSQL
- Located in src/backend/commands/tablecmds.c:14883-14894