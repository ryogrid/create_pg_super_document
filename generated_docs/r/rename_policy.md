# rename_policy

## Location
[src/backend/commands/policy.c:1096-1203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L1096-L1203)

## Overview
Changes the name of a policy on a relation by updating the policy's name in the pg_policy system catalog while ensuring the new name doesn't conflict with existing policies on the same table.

## Definition

```c
ObjectAddress
rename_policy(RenameStmt *stmt)
```
## Detailed Description
This function implements policy renaming through a two-phase process to ensure data consistency:

1. **Conflict Detection Phase**: First scans pg_policy to check if a policy with the target name already exists on the same table, raising an error if found
2. **Rename Phase**: Locates the policy with the old name and updates its name field in the catalog
3. **Atomic Operation**: Uses proper catalog locking (RowExclusiveLock) to prevent concurrent modifications during the rename process
4. **Cache Invalidation**: Invalidates the relation's cache entry to ensure all backends rebuild their policy information
5. **Event Notification**: Triggers post-alter hooks for proper event handling

The function operates directly on the catalog tuple by copying it, modifying the name field, and updating it back to the catalog.

## Parameters
- : RenameStmt structure containing:
  - : Target table (RangeVar) containing the policy to rename
  - : Current name of the policy to be renamed
  - : New name for the policy
  - Other RenameStmt fields (not directly used for policies)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md), RangeVarCallbackForPolicy (table identification and permissions)
  - [relation_open](relation_open.md), table_open, relation_close, table_close (relation management)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan (catalog scanning)
  - [heap_copytuple](../h/heap_copytuple.md) (tuple duplication)
  - [namestrcpy](../n/namestrcpy.md) (name field modification)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - InvokeObjectPostAlterHook (event hooks)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md) (cache management)
  - ObjectAddressSet (return value construction)
- Called from:
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (main rename command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on the target table to prevent concurrent DDL operations
- Policy names must be unique within each table but can be duplicated across different tables
- Uses two separate catalog scans for safety: first to check conflicts, second to perform the rename
- Does not affect policy dependencies or other attributes - only the name is changed
- Returns ObjectAddress of the renamed policy for use by the event system
- The function ensures proper cleanup of scan resources and relation locks in all code paths
- Cache invalidation ensures that query plans using the renamed policy are recompiled

## Simplified Source

```c
ObjectAddress
rename_policy(RenameStmt *stmt)
{
    Relation pg_policy_rel;
    Relation target_table;
    Oid table_id;
    Oid opoloid;
    ScanKeyData skey[2];
    SysScanDesc sscan;
    HeapTuple policy_tuple;
    ObjectAddress address;

    // Get table ID and check permissions
    table_id = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock, 0,
                                        RangeVarCallbackForPolicy, (void *) stmt);

    target_table = relation_open(table_id, NoLock);
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    // Phase 1: Check if new name already exists
    ScanKeyInit(&skey[0], Anum_pg_policy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(table_id));
    ScanKeyInit(&skey[1], Anum_pg_policy_polname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(stmt->newname));

    sscan = systable_beginscan(pg_policy_rel, PolicyPolrelidPolnameIndexId, true, NULL, 2, skey);

    if (HeapTupleIsValid(systable_getnext(sscan)))
        ereport(ERROR, "policy with new name already exists");

    systable_endscan(sscan);

    // Phase 2: Find existing policy and rename it
    ScanKeyInit(&skey[0], Anum_pg_policy_polrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(table_id));
    ScanKeyInit(&skey[1], Anum_pg_policy_polname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(stmt->subname));

    sscan = systable_beginscan(pg_policy_rel, PolicyPolrelidPolnameIndexId, true, NULL, 2, skey);
    policy_tuple = systable_getnext(sscan);

    if (!HeapTupleIsValid(policy_tuple))
        ereport(ERROR, "policy does not exist");

    // Extract policy OID and copy tuple for modification
    opoloid = ((Form_pg_policy) GETSTRUCT(policy_tuple))->oid;
    policy_tuple = heap_copytuple(policy_tuple);

    // Update the policy name
    namestrcpy(&((Form_pg_policy) GETSTRUCT(policy_tuple))->polname, stmt->newname);
    CatalogTupleUpdate(pg_policy_rel, &policy_tuple->t_self, policy_tuple);

    // Cleanup and cache invalidation
    InvokeObjectPostAlterHook(PolicyRelationId, opoloid, 0);
    ObjectAddressSet(address, PolicyRelationId, opoloid);
    CacheInvalidateRelcache(target_table);

    systable_endscan(sscan);
    table_close(pg_policy_rel, RowExclusiveLock);
    relation_close(target_table, NoLock);

    return address;
}
```