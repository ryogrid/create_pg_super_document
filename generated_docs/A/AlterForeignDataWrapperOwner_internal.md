# AlterForeignDataWrapperOwner_internal

## Location
[src/backend/commands/foreigncmds.c:216-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L216-L285)

## Overview
Internal workhorse function that changes the ownership of a foreign data wrapper, enforcing superuser privileges for both current and new owners while updating the catalog and dependency records.

## Definition
```c
static void AlterForeignDataWrapperOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
```

## Detailed Description
This static function implements the core logic for changing a foreign data wrapper's owner. It enforces strict security requirements: only superusers can change FDW ownership, and the new owner must also be a superuser. The function performs several key operations: validates permissions, updates the fdwowner field in the pg_foreign_data_wrapper catalog, adjusts any existing ACL (access control list) to reflect the new ownership, updates the catalog tuple, and maintains dependency tracking through changeDependencyOnOwner. The function also triggers post-alter hooks to notify other parts of the system about the ownership change. If the new owner is the same as the current owner, the function skips the update process entirely.

## Parameters / Member Variables
- `rel`: Open relation handle for pg_foreign_data_wrapper catalog table
- `tup`: HeapTuple representing the foreign data wrapper record to modify
- `newOwnerId`: OID of the user who will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_foreign_data_wrapper (structure for catalog tuple data)
  - [superuser](../s/superuser.md) (checks if current user is superuser)
  - [superuser_arg](../s/superuser_arg.md) (checks if specified user is superuser)
  - [heap_getattr](../h/heap_getattr.md) (retrieves attribute value from tuple)
  - [aclnewowner](../a/aclnewowner.md) (updates ACL ownership)
  - DatumGetAclP (converts Datum to ACL pointer)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (creates modified tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates catalog with new tuple)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md) (updates dependency tracking)
  - InvokeObjectPostAlterHook (triggers post-alter notifications)
- Called from (representative examples):
  - [AlterForeignDataWrapperOwner](AlterForeignDataWrapperOwner.md) (src/backend/commands/foreigncmds.c:307)
  - [AlterForeignDataWrapperOwner_oid](AlterForeignDataWrapperOwner_oid.md) (src/backend/commands/foreigncmds.c:338)

## Notes and Other Information
- Requires superuser privileges for both current user and new owner due to security implications of FDW ownership
- Only performs updates if the new owner differs from the current owner
- Properly handles NULL ACLs by skipping ACL updates when no ACL exists
- Updates both the catalog record and the system dependency tracking to maintain referential integrity
- Uses array-based tuple modification approach with repl_val, repl_null, and repl_repl arrays
- This is an internal static function used by the public ownership change functions

## Simplified Source

```c
static void AlterForeignDataWrapperOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId) {
    Form_pg_foreign_data_wrapper form = (Form_pg_foreign_data_wrapper) GETSTRUCT(tup);

    // Both current user and new owner must be superusers
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to change owner of foreign-data wrapper \"%s\"",
                              NameStr(form->fdwname)),
                       errhint("Must be superuser to change owner of a foreign-data wrapper.")));

    if (!superuser_arg(newOwnerId))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to change owner of foreign-data wrapper \"%s\"",
                              NameStr(form->fdwname)),
                       errhint("The owner of a foreign-data wrapper must be a superuser.")));

    // Only proceed if owner actually changes
    if (form->fdwowner != newOwnerId) {
        // Prepare tuple update arrays
        Datum repl_val[Natts_pg_foreign_data_wrapper];
        bool repl_null[Natts_pg_foreign_data_wrapper];
        bool repl_repl[Natts_pg_foreign_data_wrapper];

        memset(repl_null, false, sizeof(repl_null));
        memset(repl_repl, false, sizeof(repl_repl));

        // Update owner field
        repl_repl[Anum_pg_foreign_data_wrapper_fdwowner - 1] = true;
        repl_val[Anum_pg_foreign_data_wrapper_fdwowner - 1] = ObjectIdGetDatum(newOwnerId);

        // Update ACL if it exists
        Datum aclDatum = heap_getattr(tup, Anum_pg_foreign_data_wrapper_fdwacl,
                                     RelationGetDescr(rel), &isNull);
        if (!isNull) {
            Acl *newAcl = aclnewowner(DatumGetAclP(aclDatum), form->fdwowner, newOwnerId);
            repl_repl[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;
            repl_val[Anum_pg_foreign_data_wrapper_fdwacl - 1] = PointerGetDatum(newAcl);
        }

        // Update catalog and dependencies
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
        CatalogTupleUpdate(rel, &tup->t_self, tup);
        changeDependencyOnOwner(ForeignDataWrapperRelationId, form->oid, newOwnerId);
    }

    // Notify other subsystems
    InvokeObjectPostAlterHook(ForeignDataWrapperRelationId, form->oid, 0);
}
```