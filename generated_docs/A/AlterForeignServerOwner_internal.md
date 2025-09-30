# AlterForeignServerOwner_internal

## Location
[src/backend/commands/foreigncmds.c:349-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L349-L425)

## Overview
Internal workhorse function for changing a foreign server's owner, performing ownership validation checks and updating catalog records.

## Definition

```c
static void
AlterForeignServerOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
```
## Detailed Description
This internal function handles the core logic for changing the ownership of a foreign server. It performs comprehensive permission checks to ensure the operation is authorized, validates that the new owner has appropriate privileges on the associated foreign-data wrapper, and updates both the server ownership and access control list (ACL) in the catalog. The function follows PostgreSQL's standard pattern for ownership changes by checking current ownership, validating permissions, and updating dependency records.

## Parameters / Member Variables
- : Relation object for the pg_foreign_server catalog table
- : HeapTuple representing the foreign server record to be modified  
- : Object ID of the new owner to be assigned to the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md): Check if current user has superuser privileges
  - [object_ownercheck](../o/object_ownercheck.md): Verify current user owns the foreign server
  - [check_can_set_role](../c/check_can_set_role.md): Validate ability to become the new owner
  - [object_aclcheck](../o/object_aclcheck.md): Check new owner has USAGE privilege on FDW
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md): Retrieve FDW information for error reporting
  - [aclnewowner](../a/aclnewowner.md): Update ACL with new owner information
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Create modified tuple with new ownership
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Update the catalog record
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md): Update ownership dependency records
  - InvokeObjectPostAlterHook: Trigger post-alter hooks
- Called from (representative examples):
  - [AlterForeignServerOwner](AlterForeignServerOwner.md): Public interface for ownership changes
  - [AlterForeignServerOwner_oid](AlterForeignServerOwner_oid.md): OID-based ownership change wrapper

## Notes and Other Information
- Only updates ownership if the current and new owners differ
- Superusers can bypass most permission checks
- Non-superusers must own the server and be able to become the new owner
- New owner must have USAGE privilege on the associated foreign-data wrapper
- Handles ACL updates only when existing ACL is non-null
- Uses standard PostgreSQL catalog update patterns with tuple modification
- Triggers post-alter hooks for proper event notification

## Simplified Source

```c
static void AlterForeignServerOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId) {
    Form_pg_foreign_server form = (Form_pg_foreign_server) GETSTRUCT(tup);

    // Only proceed if owner actually changes
    if (form->srvowner != newOwnerId) {
        // Superusers can always change ownership
        if (!superuser()) {
            // Non-superusers must own the server
            if (!object_ownercheck(ForeignServerRelationId, form->oid, GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER, NameStr(form->srvname));

            // Must be able to become new owner
            check_can_set_role(GetUserId(), newOwnerId);

            // New owner must have USAGE privilege on the FDW
            AclResult aclresult = object_aclcheck(ForeignDataWrapperRelationId, form->srvfdw,
                                                 newOwnerId, ACL_USAGE);
            if (aclresult != ACLCHECK_OK) {
                ForeignDataWrapper *fdw = GetForeignDataWrapper(form->srvfdw);
                aclcheck_error(aclresult, OBJECT_FDW, fdw->fdwname);
            }
        }

        // Prepare tuple update arrays
        Datum repl_val[Natts_pg_foreign_server];
        bool repl_null[Natts_pg_foreign_server];
        bool repl_repl[Natts_pg_foreign_server];

        memset(repl_null, false, sizeof(repl_null));
        memset(repl_repl, false, sizeof(repl_repl));

        // Update owner field
        repl_repl[Anum_pg_foreign_server_srvowner - 1] = true;
        repl_val[Anum_pg_foreign_server_srvowner - 1] = ObjectIdGetDatum(newOwnerId);

        // Update ACL if it exists
        Datum aclDatum = heap_getattr(tup, Anum_pg_foreign_server_srvacl,
                                     RelationGetDescr(rel), &isNull);
        if (!isNull) {
            Acl *newAcl = aclnewowner(DatumGetAclP(aclDatum), form->srvowner, newOwnerId);
            repl_repl[Anum_pg_foreign_server_srvacl - 1] = true;
            repl_val[Anum_pg_foreign_server_srvacl - 1] = PointerGetDatum(newAcl);
        }

        // Update catalog and dependencies
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
        CatalogTupleUpdate(rel, &tup->t_self, tup);
        changeDependencyOnOwner(ForeignServerRelationId, form->oid, newOwnerId);
    }

    // Notify other subsystems
    InvokeObjectPostAlterHook(ForeignServerRelationId, form->oid, 0);
}
```