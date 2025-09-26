# RenameSchema

## Location
[src/backend/commands/schemacmds.c:249-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/schemacmds.c#L249-L306)

## Overview
RenameSchema implements the ALTER SCHEMA RENAME operation, changing a schema's name while performing comprehensive validation and security checks.

## Definition

```c
ObjectAddress
RenameSchema(const char *oldname, const char *newname)
```
## Detailed Description
RenameSchema handles the renaming of an existing database schema by updating the schema's name in the pg_namespace system catalog. The function performs extensive validation including ownership verification, privilege checking, and name conflict detection. It ensures that only authorized users can rename schemas and that the new name doesn't conflict with existing schemas or reserved system names.

Key behaviors include:
- Looking up the existing schema by name in pg_namespace
- Verifying the user owns the schema being renamed
- Checking for CREATE privilege on the current database
- Ensuring the new name doesn't already exist as a schema
- Validating against reserved schema names (pg_* prefix)
- Updating the catalog entry and invoking post-alter hooks
- Proper cleanup of heap tuples and relation locks

## Parameters / Member Variables
- : Current name of the schema to be renamed
- : Desired new name for the schema

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close (catalog table access with locking)
  - SearchSysCacheCopy1 (lookup existing schema by name)
  - [get_namespace_oid](../g/get_namespace_oid.md) (check for name conflicts)
  - [object_ownercheck](../o/object_ownercheck.md) (verify schema ownership)
  - [object_aclcheck](../o/object_aclcheck.md) (check CREATE privilege on database)
  - [IsReservedName](../I/IsReservedName.md) (validate against system reserved names)
  - [namestrcpy](../n/namestrcpy.md) (update schema name in catalog tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (persist changes to catalog)
  - InvokeObjectPostAlterHook (trigger post-alter hooks)
  - [heap_freetuple](../h/heap_freetuple.md) (cleanup allocated tuple memory)
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (general rename statement execution)

## Notes and Other Information
- Returns ObjectAddress identifying the renamed schema
- Requires both schema ownership and CREATE privilege on the database
- Uses RowExclusiveLock on NamespaceRelationId during the operation
- Validates that the new name doesn't conflict with existing schemas or reserved names
- Triggers post-alter hooks for potential extension or trigger processing
- Performs proper memory management by freeing the copied heap tuple