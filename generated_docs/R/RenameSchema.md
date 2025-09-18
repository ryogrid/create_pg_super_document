# RenameSchema

## Location
src/backend/commands/schemacmds.c: 249 - 306

## Overview
RenameSchema implements the ALTER SCHEMA RENAME operation, changing a schema's name while performing comprehensive validation and security checks.

## Definition


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
  - table_open/table_close (catalog table access with locking)
  - SearchSysCacheCopy1 (lookup existing schema by name)
  - get_namespace_oid (check for name conflicts)
  - object_ownercheck (verify schema ownership)
  - object_aclcheck (check CREATE privilege on database)
  - IsReservedName (validate against system reserved names)
  - namestrcpy (update schema name in catalog tuple)
  - CatalogTupleUpdate (persist changes to catalog)
  - InvokeObjectPostAlterHook (trigger post-alter hooks)
  - heap_freetuple (cleanup allocated tuple memory)
- Called from (representative examples):
  - ExecRenameStmt (general rename statement execution)

## Notes and Other Information
- Returns ObjectAddress identifying the renamed schema
- Requires both schema ownership and CREATE privilege on the database
- Uses RowExclusiveLock on NamespaceRelationId during the operation
- Validates that the new name doesn't conflict with existing schemas or reserved names
- Triggers post-alter hooks for potential extension or trigger processing
- Performs proper memory management by freeing the copied heap tuple