# AlterSchemaOwner_internal

## Location
[src/backend/commands/schemacmds.c:361-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/schemacmds.c#L361-L442)

## Overview
AlterSchemaOwner_internal performs the core logic for changing schema ownership, handling security validation, ACL updates, and dependency maintenance.

## Definition

```c
static void
AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId)
```
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
- `tup`: HeapTuple representing the schema record from pg_namespace catalog
- `rel`: Open Relation handle for the pg_namespace catalog (must have RowExclusiveLock)
- `newOwnerId`: OID of the role that should become the new owner
## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md) (validates current user owns the schema)
  - [check_can_set_role](../c/check_can_set_role.md) (ensures user can become the target role)
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
- [Unique](../U/Unique.md) security model requiring CREATE privilege from current user rather than target owner
- Handles ACL updates only when existing ACL is non-null, preserving NULL ACL semantics
- Updates both the ownership field and corresponding shared dependencies atomically
- Uses heap_modify_tuple pattern for safe catalog updates with proper tuple replacement

## Simplified Source

```c
static void AlterSchemaOwner_internal(HeapTuple tup, Relation rel, Oid newOwnerId) {
    Form_pg_namespace nspForm = (Form_pg_namespace) GETSTRUCT(tup);

    // Only proceed if owner actually changes
    if (nspForm->nspowner != newOwnerId) {
        // Must be owner of the existing schema
        if (!object_ownercheck(NamespaceRelationId, nspForm->oid, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA, NameStr(nspForm->nspname));

        // Must be able to become new owner
        check_can_set_role(GetUserId(), newOwnerId);

        // Current user must have CREATE privilege on database
        // (unique requirement for schema ownership changes)
        AclResult aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId,
                                             GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId));

        // Prepare tuple update arrays
        Datum repl_val[Natts_pg_namespace];
        bool repl_null[Natts_pg_namespace];
        bool repl_repl[Natts_pg_namespace];

        memset(repl_null, false, sizeof(repl_null));
        memset(repl_repl, false, sizeof(repl_repl));

        // Update owner field
        repl_repl[Anum_pg_namespace_nspowner - 1] = true;
        repl_val[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(newOwnerId);

        // Update ACL if it exists
        Datum aclDatum = SysCacheGetAttr(NAMESPACENAME, tup, Anum_pg_namespace_nspacl, &isNull);
        if (!isNull) {
            Acl *newAcl = aclnewowner(DatumGetAclP(aclDatum), nspForm->nspowner, newOwnerId);
            repl_repl[Anum_pg_namespace_nspacl - 1] = true;
            repl_val[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(newAcl);
        }

        // Update catalog and dependencies
        HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(rel),
                                              repl_val, repl_null, repl_repl);
        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
        heap_freetuple(newtuple);
        changeDependencyOnOwner(NamespaceRelationId, nspForm->oid, newOwnerId);
    }

    // Notify other subsystems
    InvokeObjectPostAlterHook(NamespaceRelationId, nspForm->oid, 0);
}
```