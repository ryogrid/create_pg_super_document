# AlterPublication

## Location
[src/backend/commands/publicationcmds.c:1371-1440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1371-L1440)

## Overview
AlterPublication is the main dispatcher function for modifying existing publications, coordinating table/schema changes and option updates while enforcing security and consistency constraints.

## Definition

```c
void
AlterPublication(ParseState *pstate, AlterPublicationStmt *stmt)
```
## Detailed Description
This function serves as the central coordinator for all publication modification operations in PostgreSQL's logical replication system. It handles both option changes (via AlterPublicationOptions) and structural changes involving tables and schemas (via AlterPublicationTables and AlterPublicationSchemas). The function implements a robust locking strategy to handle concurrent DDL operations and ensures ownership verification before allowing modifications. It performs publication lookup, validates user permissions, and delegates to specialized functions based on the type of alteration requested.

## Parameters / Member Variables
- : ParseState pointer containing parsing context and source text information for the statement
- : AlterPublicationStmt pointer specifying the publication name, action type, objects to modify, and options

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (relation access)
  - SearchSysCacheCopy1 (catalog lookup)
  - [object_ownercheck](../o/object_ownercheck.md) (permission verification)
  - [AlterPublicationOptions](AlterPublicationOptions.md) (option modifications)
  - [ObjectsInPublicationToOids](../O/ObjectsInPublicationToOids.md) (object resolution)
  - [CheckAlterPublication](../C/CheckAlterPublication.md) (validation)
  - [AlterPublicationTables](AlterPublicationTables.md) (table management)
  - [AlterPublicationSchemas](AlterPublicationSchemas.md) (schema management)
  - [LockDatabaseObject](../L/LockDatabaseObject.md) (concurrency control)
  - [heap_freetuple](../h/heap_freetuple.md) (memory management)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires ownership of the publication to perform modifications
- Uses RowExclusiveLock on publication relation and AccessExclusiveLock on individual publication
- Implements double-lookup pattern to handle concurrent DDL (checks existence after acquiring lock)
- Delegates to specialized functions: AlterPublicationOptions for options, AlterPublicationTables/AlterPublicationSchemas for structural changes
- Part of PostgreSQL's logical replication infrastructure, handling both DDL and configuration changes
- Error handling includes checks for non-existent publications and permission violations