# ReplaceRoleInInitPriv

## Location
[src/backend/catalog/aclchk.c:4813-4921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4813-L4921)

## Overview
A function used by shdepReassignOwned to replace mentions of a role in pg_init_privs entries during role reassignment operations.

## Definition
void ReplaceRoleInInitPriv(Oid oldroleid, Oid newroleid, Oid classid, Oid objid, int32 objsubid)

## Detailed Description
This function is specifically designed to support role reassignment operations by updating initial privilege records stored in pg_init_privs. When a role is being reassigned to another role, this function locates the corresponding initial privilege entry and replaces all occurrences of the old role ID with the new role ID within the ACL. The function uses aclnewowner to perform the role replacement, which handles both ownership and grantee relationships within the ACL structure.

If the role replacement results in an empty ACL, the entire pg_init_privs entry is deleted. The function also maintains dependency tracking by updating pg_shdepend records to reflect the new role relationships.

## Parameters / Member Variables
- oldroleid: The OID of the role being replaced in the ACL
- newroleid: The OID of the role that will replace the old role
- classid: The OID of the system catalog table that defines the object type
- objid: The OID of the object whose initial privileges are being updated
- objsubid: Sub-object identifier (0 for objects without sub-components)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetAclPCopy
  - [aclnewowner](../a/aclnewowner.md)
  - ACL_NUM
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [aclmembers](../a/aclmembers.md)
  - [updateInitAclDependencies](../u/updateInitAclDependencies.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [shdepReassignOwned_InitAcl](../s/shdepReassignOwned_InitAcl.md)

## Notes and Other Information
- Specifically designed for role reassignment operations via shdepReassignOwned
- Uses aclnewowner in an off-label manner to replace any role, not just owners
- Gracefully handles cases where no pg_init_privs entry exists for the object
- Deletes the entire entry if role replacement results in an empty ACL
- Maintains consistency with pg_shdepend through updateInitAclDependencies
- Uses CommandCounterIncrement to handle multiple object processing
- Requires RowExclusiveLock on pg_init_privs to ensure update consistency

## Simplified Source

```c
void
ReplaceRoleInInitPriv(Oid oldroleid, Oid newroleid,
                      Oid classid, Oid objid, int32 objsubid) {
    Relation rel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple oldtuple;

    // Open pg_init_privs catalog
    rel = table_open(InitPrivsRelationId, RowExclusiveLock);

    // Set up search keys for the target object
    ScanKeyInit(&key[0], Anum_pg_init_privs_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
    ScanKeyInit(&key[1], Anum_pg_init_privs_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
    ScanKeyInit(&key[2], Anum_pg_init_privs_objsubid,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    // Search for existing entry
    scan = systable_beginscan(rel, InitPrivsObjIndexId, true, NULL, 3, key);
    oldtuple = systable_getnext(scan);

    if (!HeapTupleIsValid(oldtuple)) {
        // No entry found - nothing to do
        systable_endscan(scan);
        table_close(rel, RowExclusiveLock);
        return;
    }

    // Get current ACL and replace the role
    Datum oldAclDatum = heap_getattr(oldtuple, Anum_pg_init_privs_initprivs,
                                    RelationGetDescr(rel), &isNull);
    Acl *old_acl = DatumGetAclPCopy(oldAclDatum);
    Acl *new_acl = aclnewowner(old_acl, oldroleid, newroleid);

    // Delete entry if ACL becomes empty, otherwise update it
    if (new_acl == NULL || ACL_NUM(new_acl) == 0) {
        CatalogTupleDelete(rel, &oldtuple->t_self);
    } else {
        // Update with new ACL
        Datum values[Natts_pg_init_privs] = {0};
        bool nulls[Natts_pg_init_privs] = {0};
        bool replaces[Natts_pg_init_privs] = {0};

        values[Anum_pg_init_privs_initprivs - 1] = PointerGetDatum(new_acl);
        replaces[Anum_pg_init_privs_initprivs - 1] = true;

        HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel),
                                              values, nulls, replaces);
        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
    }

    // Update dependency information
    int noldmembers, nnewmembers;
    Oid *oldmembers, *newmembers;
    noldmembers = aclmembers(old_acl, &oldmembers);
    nnewmembers = aclmembers(new_acl, &newmembers);

    updateInitAclDependencies(classid, objid, objsubid,
                             noldmembers, oldmembers,
                             nnewmembers, newmembers);

    systable_endscan(scan);
    CommandCounterIncrement();
    table_close(rel, RowExclusiveLock);
}
```