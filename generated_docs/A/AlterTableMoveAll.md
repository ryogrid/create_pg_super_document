# AlterTableMoveAll

## Location
[src/backend/commands/tablecmds.c:15385-15546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15385-L15546)

## Overview
AlterTableMoveAll implements the ALTER TABLE ALL ... SET TABLESPACE command, allowing batch movement of all objects of a specified type from one tablespace to another, with optional filtering by object owner.

## Definition
```c
Oid AlterTableMoveAll(AlterTableMoveAllStmt *stmt)
```

## Detailed Description
This function provides bulk tablespace migration functionality for database objects by scanning the pg_class system catalog to find all relations of the specified type in the source tablespace and moving them to the destination tablespace. It supports filtering by object type (tables, indexes, materialized views) and owner roles, performing comprehensive permission checks and locking operations to ensure safe concurrent execution.

The function operates in phases: first validating tablespace permissions and resolving OIDs, then scanning pg_class to identify candidate objects while applying various filters (object type, ownership, system restrictions), collecting and locking all target relations, and finally executing individual ALTER TABLE commands for each relation. The process includes safeguards against moving system catalogs, shared tables, temporary tables, and TOAST tables, which are handled automatically with their parent tables.

## Parameters / Member Variables
- `stmt`: AlterTableMoveAllStmt structure containing the command parameters including source tablespace, destination tablespace, object type filter, role filters, and NOWAIT option

## Dependencies
- Functions called/Symbols referenced:
  - [roleSpecsToIds](../r/roleSpecsToIds.md): Converts role specifications to OID list
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Resolves tablespace names to OIDs
  - [object_aclcheck](../o/object_aclcheck.md): Checks user permissions on tablespaces
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission-related errors
  - table_open/table_close: Opens and closes system catalogs
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Starts catalog scan
  - [heap_getnext](../h/heap_getnext.md): Retrieves next tuple from scan
  - [IsCatalogNamespace](../I/IsCatalogNamespace.md): Checks if namespace is system catalog
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md): Checks if namespace is temporary
  - [IsToastNamespace](../I/IsToastNamespace.md): Checks if namespace is for TOAST tables
  - [object_ownercheck](../o/object_ownercheck.md): Verifies object ownership
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md): Attempts non-blocking lock acquisition
  - [LockRelationOid](../L/LockRelationOid.md): Acquires exclusive lock on relation
  - [AlterTableInternal](AlterTableInternal.md): Executes individual ALTER TABLE operations
  - [EventTriggerAlterTableStart](../E/EventTriggerAlterTableStart.md)/End: Manages event trigger execution

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing function

## Notes and Other Information
- Supports three object types: tables (including partitioned tables), indexes (including partitioned indexes), and materialized views
- Automatically excludes system catalogs, shared relations, temporary tables, and TOAST tables from bulk operations
- Requires CREATE permission on destination tablespace and ownership of each object being moved
- Implements NOWAIT semantics for lock acquisition to avoid blocking on busy objects
- Performs no-op detection when source and destination tablespaces are identical
- Uses AccessExclusiveLock to prevent concurrent modifications during the move operation
- Handles database default tablespace by converting to InvalidOid for internal representation
- Provides informative notice when no matching objects are found in the source tablespace
- Integrates with event trigger system for proper dependency tracking and custom logic execution
- Returns the destination tablespace OID upon successful completion