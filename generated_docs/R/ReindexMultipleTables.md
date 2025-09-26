# ReindexMultipleTables

## Location
[src/backend/commands/indexcmds.c:2977-3195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2977-L3195)

## Overview
ReindexMultipleTables recreates indexes for multiple tables selected by objectName/objectKind (schema, database, or system catalogs) with each table processed in a separate transaction to reduce deadlock probability.

## Definition
```c
static void ReindexMultipleTables(const ReindexStmt *stmt, const ReindexParams *params)
```

## Detailed Description
This function orchestrates bulk reindexing operations across multiple tables within a specified scope (schema, database, or system catalogs). Key behaviors include:

1. **Scope Validation**: Validates the target object (schema, database, or system catalogs) and checks appropriate permissions
2. **Permission Checking**: Performs different permission checks based on object type (schema ownership, database ownership, or ROLE_PG_MAINTAIN privileges)
3. **Table Discovery**: Scans pg_class to identify candidate tables for reindexing based on the specified scope
4. **Filtering Logic**: Applies multiple filters to exclude inappropriate relations:
   - Only processes regular tables (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)  
   - Skips temporary tables from other backends
   - Handles system vs user catalogs based on object kind
   - Enforces concurrent reindexing restrictions for system catalogs
   - Manages tablespace restrictions for mapped relations and system tables
5. **Transaction Management**: Processes each relation in a separate transaction to minimize deadlock risk
6. **Ordering Optimization**: Prioritizes pg_class to ensure catalog integrity before processing other relations

## Parameters / Member Variables
- `stmt`: ReindexStmt containing reindex statement details including target object name and kind
- `params`: ReindexParams specifying reindex options like concurrency, tablespace, and other flags

## Dependencies
- Functions called/Symbols referenced:
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_database_name](../g/get_database_name.md)
  - AllocSetContextCreate
  - [table_open](../t/table_open.md)/table_close
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [IsCatalogRelationOid](../I/IsCatalogRelationOid.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [ReindexMultipleInternal](ReindexMultipleInternal.md)
- Called from:
  - [ExecReindex](../E/ExecReindex.md)

## Notes and Other Information
- Must not be called within a user transaction block due to internal transaction commits
- Concurrent reindexing of system catalogs is explicitly prohibited with an error
- The function creates a private memory context to survive transaction commits
- pg_class is always reindexed first when selected to ensure catalog integrity
- Provides warnings for skipped relations due to concurrent or tablespace restrictions
- Uses separate transactions for each table to reduce deadlock probability and allow immediate lock release
- Supports filtering by relation persistence (temporary vs permanent) and ownership checks for shared catalogs