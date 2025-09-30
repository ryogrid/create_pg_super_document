# RemoveRoleFromInitPriv

## Location
[src/backend/catalog/aclchk.c:4922-5049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4922-L5049)

## Overview
A function used by shdepDropOwned to remove mentions of a role from pg_init_privs entries when a role is being dropped.

## Definition
void RemoveRoleFromInitPriv(Oid roleid, Oid classid, Oid objid, int32 objsubid)

## Detailed Description
This function is designed to support role dropping operations by removing all references to a specific role from initial privilege records stored in pg_init_privs. When a role is being dropped, this function locates the corresponding initial privilege entry and removes all privileges granted to or by the specified role. The function uses merge_acl_with_grant with is_grant=false to effectively revoke all privileges associated with the role.

The function requires determining the object's owner to properly process the ACL modifications, using the system cache to look up owner information. If removing the role results in an empty ACL, the entire pg_init_privs entry is deleted. The function maintains dependency tracking by updating pg_shdepend records to reflect the role removal.

## Parameters / Member Variables
- roleid: The OID of the role to be removed from the ACL
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
  - [aclmembers](../a/aclmembers.md)
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache
  - [get_object_class_descr](../g/get_object_class_descr.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [get_object_attnum_owner](../g/get_object_attnum_owner.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - DROP_RESTRICT
  - list_make1_oid
  - ACLITEM_ALL_PRIV_BITS
  - ACL_NUM
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [updateInitAclDependencies](../u/updateInitAclDependencies.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [shdepDropOwned](../s/shdepDropOwned.md)

## Notes and Other Information
- Specifically designed for role dropping operations via shdepDropOwned
- Must determine object owner through system cache lookups to properly process ACLs
- Uses merge_acl_with_grant in revoke mode (is_grant=false) to remove role privileges
- Gracefully handles cases where no pg_init_privs entry exists for the object
- Deletes the entire entry if role removal results in an empty ACL
- Uses DROP_RESTRICT mode and ACLITEM_ALL_PRIV_BITS to remove all privileges
- Maintains consistency with pg_shdepend through updateInitAclDependencies
- Requires RowExclusiveLock on pg_init_privs to ensure update consistency
- Uses CommandCounterIncrement to handle multiple object processing

## Simplified Source
```c
void
RemoveRoleFromInitPriv(Oid roleid, Oid classid, Oid objid, int32 objsubid)
{
    Relation rel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple oldtuple;
    Acl *old_acl;
    Acl *new_acl;
    Oid ownerId;

    // Open pg_init_privs table and search for target object
    rel = table_open(InitPrivsRelationId, RowExclusiveLock);

    ScanKeyInit(&key[0], Anum_pg_init_privs_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
    ScanKeyInit(&key[1], Anum_pg_init_privs_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
    ScanKeyInit(&key[2], Anum_pg_init_privs_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    scan = systable_beginscan(rel, InitPrivsObjIndexId, true, NULL, 3, key);
    oldtuple = systable_getnext(scan);

    if (!HeapTupleIsValid(oldtuple))
    {
        // No entry found - nothing to do
        systable_endscan(scan);
        table_close(rel, RowExclusiveLock);
        return;
    }

    // Get current ACL and extract member info for dependency tracking
    Datum oldAclDatum = heap_getattr(oldtuple, Anum_pg_init_privs_initprivs, RelationGetDescr(rel), &isNull);
    old_acl = DatumGetAclPCopy(oldAclDatum);

    int noldmembers = aclmembers(old_acl, &oldmembers);

    // Look up object owner
    int cacheid = get_object_catcache_oid(classid);
    HeapTuple objtuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objid));
    ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(cacheid, objtuple, get_object_attnum_owner(classid)));
    ReleaseSysCache(objtuple);

    // Remove role from ACL by revoking all its privileges
    if (old_acl != NULL)
        new_acl = merge_acl_with_grant(old_acl, false, false, DROP_RESTRICT,
                                      list_make1_oid(roleid), ACLITEM_ALL_PRIV_BITS, ownerId, ownerId);
    else
        new_acl = NULL;

    // Update or delete entry based on resulting ACL
    if (new_acl == NULL || ACL_NUM(new_acl) == 0)
    {
        // Empty ACL - delete entire entry
        CatalogTupleDelete(rel, &oldtuple->t_self);
    }
    else
    {
        // Update entry with new ACL
        Datum values[Natts_pg_init_privs] = {0};
        bool nulls[Natts_pg_init_privs] = {0};
        bool replaces[Natts_pg_init_privs] = {0};

        values[Anum_pg_init_privs_initprivs - 1] = PointerGetDatum(new_acl);
        replaces[Anum_pg_init_privs_initprivs - 1] = true;

        HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
    }

    // Update shared dependency information
    int nnewmembers = aclmembers(new_acl, &newmembers);
    updateInitAclDependencies(classid, objid, objsubid, noldmembers, oldmembers, nnewmembers, newmembers);

    systable_endscan(scan);
    CommandCounterIncrement();
    table_close(rel, RowExclusiveLock);
}
```