# AlterSchemaOwner_oid

## Location
src/backend/commands/schemacmds.c: 307 - 329

## Overview
AlterSchemaOwner_oid changes the owner of a schema identified by its OID, serving as a lightweight wrapper around the internal ownership change logic.

## Definition


## Detailed Description
AlterSchemaOwner_oid provides a simple interface for changing schema ownership when the schema is identified by its OID rather than name. This function handles the catalog lookup and locking necessary to safely modify schema ownership, delegating the actual ownership change logic to AlterSchemaOwner_internal. It's primarily used in system operations where the schema OID is already known, such as during dependency reassignment operations.

Key behaviors include:
- Opening the pg_namespace catalog with appropriate locking
- Looking up the schema tuple by OID using the system cache
- Delegating to AlterSchemaOwner_internal for the actual ownership change
- Proper cleanup of system cache references and relation locks

## Parameters / Member Variables
- : OID of the schema whose ownership should be changed
- : OID of the role that should become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close (catalog access with RowExclusiveLock)
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache (schema lookup by OID)
  - [AlterSchemaOwner_internal](AlterSchemaOwner_internal.md) (performs the actual ownership change)
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md) (dependency reassignment during role operations)

## Notes and Other Information
- Void return type as it performs the operation in-place
- Uses RowExclusiveLock on NamespaceRelationId to ensure exclusive access during ownership change
- Serves as a thin wrapper around AlterSchemaOwner_internal, handling only the lookup and locking aspects
- Error handling for invalid schema OIDs through cache lookup validation
- Part of the schema ownership change infrastructure used by higher-level operations