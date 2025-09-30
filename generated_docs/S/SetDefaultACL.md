# SetDefaultACL

## Location
[src/backend/catalog/aclchk.c:1203-1465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1203-L1465)

## Overview
Creates or updates a pg_default_acl catalog entry to store default access control privileges for future objects of a specific type, role, and namespace combination.

## Definition

```c
static void
SetDefaultACL(InternalDefaultACL *iacls)
```
## Detailed Description
This function implements the core logic for managing default ACL entries in the pg_default_acl system catalog. It handles both database-wide (global) and schema-specific default privileges for various object types (tables, sequences, functions, types, schemas). The function determines the appropriate default ACL baseline - for global entries, it uses hard-wired defaults via acldefault(), while schema-specific entries start with an empty ACL. It searches for existing entries using a three-key cache lookup (role, namespace, object type), then merges the requested privilege changes with the existing ACL using merge_acl_with_grant(). If the result equals the default ACL, the entry is deleted; otherwise, it's inserted or updated. The function also manages dependency relationships and shared dependency ACL information to maintain referential integrity.

## Parameters / Member Variables
- : Pointer to InternalDefaultACL structure containing all the privilege specification details including role ID, namespace ID, object type, privileges, grantees, and grant options

## Dependencies
- Functions called/Symbols referenced:
  - [acldefault](../a/acldefault.md)
  - [make_empty_acl](../m/make_empty_acl.md)
  - [SearchSysCache3](SearchSysCache3.md)
  - [SysCacheGetAttr](SysCacheGetAttr.md)
  - DatumGetAclPCopy
  - [aclmembers](../a/aclmembers.md)
  - [aclcopy](../a/aclcopy.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [aclitemsort](../a/aclitemsort.md)
  - [aclequal](../a/aclequal.md)
  - [performDeletion](../p/performDeletion.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - InvokeObjectPostCreateHook
  - InvokeObjectPostAlterHook
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [SetDefaultACLsInSchemas](SetDefaultACLsInSchemas.md)
  - [RemoveRoleFromObjectACL](../R/RemoveRoleFromObjectACL.md)

## Notes and Other Information
- The function is static and serves as the lowest-level implementation for default ACL management
- Global entries (nspid = InvalidOid) replace hard-wired defaults, while schema-specific entries are additive
- Object types are converted from OBJECT_* constants to DEFACLOBJ_* constants for catalog storage
- The 'all_privs' flag automatically expands to the appropriate ACL_ALL_RIGHTS_* constant for the object type
- Schema-specific default privileges for schemas themselves are explicitly forbidden and will generate an error
- ACL comparison requires sorting both ACLs to ensure accurate equality checks
- The function uses RowExclusiveLock on pg_default_acl to prevent concurrent modifications
- Dependency management includes both ownership dependencies (on the role) and usage dependencies (on the namespace)
- [CommandCounterIncrement](../C/CommandCounterIncrement.md)() prevents issues when processing duplicate objects in the same command
- Post-creation/alteration hooks are invoked for proper event notification

## Simplified Source

```c
static void SetDefaultACL(InternalDefaultACL *iacls) {
    AclMode this_privileges = iacls->privileges;
    char objtype;
    Relation rel;
    HeapTuple tuple;
    bool isNew;
    Acl *def_acl, *old_acl, *new_acl;

    rel = table_open(DefaultAclRelationId, RowExclusiveLock);

    // Get default ACL baseline - global entries use hard-wired defaults,
    // schema-specific entries start empty
    if (!OidIsValid(iacls->nspid))
        def_acl = acldefault(iacls->objtype, iacls->roleid);
    else
        def_acl = make_empty_acl();

    // Convert object type and handle all_privs expansion
    switch (iacls->objtype) {
        case OBJECT_TABLE:
            objtype = DEFACLOBJ_RELATION;
            if (iacls->all_privs && this_privileges == ACL_NO_RIGHTS)
                this_privileges = ACL_ALL_RIGHTS_RELATION;
            break;
        // ... similar cases for SEQUENCE, FUNCTION, TYPE, SCHEMA
        default:
            elog(ERROR, "unrecognized object type: %d", (int) iacls->objtype);
    }

    // Look for existing catalog entry
    tuple = SearchSysCache3(DEFACLROLENSPOBJ,
                           ObjectIdGetDatum(iacls->roleid),
                           ObjectIdGetDatum(iacls->nspid),
                           CharGetDatum(objtype));

    if (HeapTupleIsValid(tuple)) {
        // Extract existing ACL from catalog
        Datum aclDatum = SysCacheGetAttr(DEFACLROLENSPOBJ, tuple,
                                        Anum_pg_default_acl_defaclacl, &isNull);
        old_acl = isNull ? NULL : DatumGetAclPCopy(aclDatum);
        isNew = false;
    } else {
        old_acl = NULL;
        isNew = true;
    }

    // Start with default if no existing ACL
    if (old_acl == NULL)
        old_acl = aclcopy(def_acl);

    // Generate new ACL by merging grant/revoke operations
    new_acl = merge_acl_with_grant(old_acl, iacls->is_grant, iacls->grant_option,
                                   iacls->behavior, iacls->grantees,
                                   this_privileges, iacls->roleid, iacls->roleid);

    // If result equals default, remove entry; otherwise insert/update
    aclitemsort(new_acl);
    aclitemsort(def_acl);

    if (aclequal(new_acl, def_acl)) {
        // Delete existing entry if present
        if (!isNew) {
            ObjectAddress myself;
            myself.classId = DefaultAclRelationId;
            myself.objectId = ((Form_pg_default_acl) GETSTRUCT(tuple))->oid;
            myself.objectSubId = 0;
            performDeletion(&myself, DROP_RESTRICT, 0);
        }
    } else {
        // Insert new or update existing entry
        if (isNew) {
            // Create new catalog entry with all required fields
            Oid defAclOid = GetNewOidWithIndex(rel, DefaultAclOidIndexId,
                                              Anum_pg_default_acl_oid);
            // Set up values array and insert tuple
            CatalogTupleInsert(rel, heap_form_tuple(RelationGetDescr(rel), values, nulls));

            // Record dependencies on role and namespace
            recordDependencyOnOwner(DefaultAclRelationId, defAclOid, iacls->roleid);
            if (OidIsValid(iacls->nspid))
                recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
        } else {
            // Update existing entry's ACL field
            CatalogTupleUpdate(rel, &newtuple->t_self,
                              heap_modify_tuple(tuple, RelationGetDescr(rel),
                                               values, nulls, replaces));
        }

        // Update shared dependency information
        updateAclDependencies(DefaultAclRelationId, defAclOid, 0, iacls->roleid,
                             noldmembers, oldmembers, nnewmembers, newmembers);
    }

    if (HeapTupleIsValid(tuple))
        ReleaseSysCache(tuple);

    table_close(rel, RowExclusiveLock);
    CommandCounterIncrement();
}
```