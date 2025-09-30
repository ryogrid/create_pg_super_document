# RemoveRoleFromObjectPolicy

## Location
[src/backend/commands/policy.c:416-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L416-L568)

## Overview
Removes a specified role from a policy's applicable roles list, maintaining policy integrity by either updating the policy with remaining roles or indicating the policy should be dropped if no roles would remain.

## Definition

```c
struct_array_builtin(role_oids, num_roles, OIDOID);
```
## Detailed Description
This function removes a role from a policy's applicable roles list stored in the pg_policy catalog. It handles several important aspects:

1. **Role Removal**: Scans through the policy's current roles array and rebuilds it without the target role
2. **Duplicate Handling**: Properly handles cases where the same role appears multiple times in the policy (historically allowed by CREATE/ALTER POLICY)
3. **Policy Preservation Logic**: Returns a boolean indicating whether the policy should be kept or dropped entirely
4. **Dependency Management**: Updates shared dependency records to reflect the new set of applicable roles
5. **Cache Invalidation**: Invalidates relation cache for the table the policy belongs to, forcing plan recompilation

The function performs atomic operations on the pg_policy system catalog and maintains referential integrity through the dependency system.

## Parameters
- : OID of the role to be removed from the policy
- : Should always be PolicyRelationId (assertion enforced)  
- : OID of the policy from which to remove the role

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (catalog scanning)
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple, heap_freetuple (tuple manipulation)
  - DatumGetArrayTypePCopy, construct_array_builtin (array operations)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md), recordSharedDependencyOn (dependency management)
  - InvokeObjectPostAlterHook (event hooks)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md), CacheInvalidateRelcacheByTuple (cache management)
- Called from:
  - [shdepDropOwned](../s/shdepDropOwned.md) (when dropping owned objects during role cleanup)

## Notes and Other Information
- Returns true if the policy should be kept (roles remain), false if it should be dropped (no roles left)
- The caller is responsible for actually dropping the policy when this function returns false
- Uses RowExclusiveLock on pg_policy to prevent concurrent modifications
- Handles race conditions gracefully (e.g., if the relation was dropped concurrently)
- Does not create dependencies on the PUBLIC role (ACL_ID_PUBLIC) as it's implicitly available

## Simplified Source
```c
bool
RemoveRoleFromObjectPolicy(Oid roleid, Oid classid, Oid policy_id)
{
    Relation pg_policy_rel;
    SysScanDesc sscan;
    ScanKeyData skey[1];
    HeapTuple tuple;
    Oid relid;
    ArrayType *policy_roles;
    Datum roles_datum;
    Oid *roles;
    int num_roles;
    Datum *role_oids;
    bool keep_policy = true;
    int i, j;

    Assert(classid == PolicyRelationId);

    // Open pg_policy catalog and find the target policy
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_policy_oid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(policy_id));
    sscan = systable_beginscan(pg_policy_rel, PolicyOidIndexId, true, NULL, 1, skey);
    tuple = systable_getnext(sscan);

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "could not find tuple for policy %u", policy_id);

    // Get relation ID and current roles array
    relid = ((Form_pg_policy) GETSTRUCT(tuple))->polrelid;
    roles_datum = heap_getattr(tuple, Anum_pg_policy_polroles, RelationGetDescr(pg_policy_rel), &attr_isnull);

    policy_roles = DatumGetArrayTypePCopy(roles_datum);
    roles = (Oid *) ARR_DATA_PTR(policy_roles);
    num_roles = ARR_DIMS(policy_roles)[0];

    // Rebuild roles array without the target role (handles duplicates)
    role_oids = (Datum *) palloc(num_roles * sizeof(Datum));
    for (i = 0, j = 0; i < num_roles; i++)
    {
        if (roles[i] != roleid)
            role_oids[j++] = ObjectIdGetDatum(roles[i]);
    }
    num_roles = j;

    if (num_roles > 0)
    {
        // Update policy with remaining roles
        ArrayType *role_ids = construct_array_builtin(role_oids, num_roles, OIDOID);

        Datum values[Natts_pg_policy] = {0};
        bool isnull[Natts_pg_policy] = {0};
        bool replaces[Natts_pg_policy] = {0};

        replaces[Anum_pg_policy_polroles - 1] = true;
        values[Anum_pg_policy_polroles - 1] = PointerGetDatum(role_ids);

        HeapTuple new_tuple = heap_modify_tuple(tuple, RelationGetDescr(pg_policy_rel), values, isnull, replaces);
        CatalogTupleUpdate(pg_policy_rel, &new_tuple->t_self, new_tuple);

        // Update shared dependencies
        deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0);

        ObjectAddress myself = {PolicyRelationId, policy_id, 0};
        ObjectAddress target = {AuthIdRelationId, 0, 0};

        for (i = 0; i < num_roles; i++)
        {
            target.objectId = DatumGetObjectId(role_oids[i]);
            if (target.objectId != ACL_ID_PUBLIC)
                recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
        }

        InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);
        heap_freetuple(new_tuple);
        CommandCounterIncrement();

        // Invalidate relation cache if relation still exists
        HeapTuple reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (HeapTupleIsValid(reltup))
        {
            CacheInvalidateRelcacheByTuple(reltup);
            ReleaseSysCache(reltup);
        }
    }
    else
    {
        // No roles would remain - policy should be dropped
        keep_policy = false;
    }

    systable_endscan(sscan);
    table_close(pg_policy_rel, RowExclusiveLock);

    return keep_policy;
}
```