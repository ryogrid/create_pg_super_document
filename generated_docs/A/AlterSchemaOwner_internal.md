# AlterSchemaOwner_internal

## Location
[src/backend/commands/schemacmds.c:361-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/schemacmds.c#L361-L442)

## Overview
AlterSchemaOwner_internal performs the core logic for changing schema ownership, handling security validation, ACL updates, and dependency maintenance.

## Definition


## Detailed Description
AlterSchemaOwner_internal implements the complete ownership transfer process for schemas, including comprehensive security checks, ACL (Access Control List) modification, and dependency updates. This internal function performs the actual work of ownership changes, validating permissions, updating catalog entries, and maintaining referential integrity. It handles both the ownership field update and the corresponding ACL adjustments to ensure proper access control under the new ownership.

Key behaviors include:
- Validating current user ownership of the schema being transferred
- Checking ability to assume the target role (preventing unauthorized ownership transfers)
- Verifying CREATE privilege on the database (unique to schema ownership changes)
- Updating the schema owner field in pg_namespace
- Modifying ACLs to reflect new ownership while preserving existing permissions
- Updating shared dependency records for the ownership change
- Triggering post-alter hooks for extension and trigger processing
- Optimizing for no-op cases where ownership doesn't actually change

## Parameters / Member Variables
- : HeapTuple representing the schema record from pg_namespace catalog
- : Open Relation handle for the pg_namespace catalog (must have RowExclusiveLock)
- : OID of the role that should become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md) (validates current user owns the schema)
  - check_can_set_role (ensures user can become the target role)
  - [object_aclcheck](../o/object_aclcheck.md) (verifies CREATE privilege on database)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (retrieves current ACL from catalog tuple)
  - [aclnewowner](../a/aclnewowner.md) (computes new ACL with updated ownership)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (creates updated catalog tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (persists changes to catalog)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md) (updates shared dependency records)
  - InvokeObjectPostAlterHook (triggers post-alter processing)
- Called from (representative examples):
  - [AlterSchemaOwner](AlterSchemaOwner.md) (name-based ownership change interface)
  - [AlterSchemaOwner_oid](AlterSchemaOwner_oid.md) (OID-based ownership change interface)

## Notes and Other Information
- Static function providing the core implementation for both public ownership change interfaces
- Includes early return optimization when new owner equals current owner (useful for dump restoration)
- Unique security model requiring CREATE privilege from current user rather than target owner
- Handles ACL updates only when existing ACL is non-null, preserving NULL ACL semantics
- Updates both the ownership field and corresponding shared dependencies atomically
- Uses heap_modify_tuple pattern for safe catalog updates with proper tuple replacement