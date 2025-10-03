# shdepDropOwned

## Location
[src/backend/catalog/pg_shdepend.c:1342-1529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1342-L1529)

## Overview
Drops all objects owned by the specified role(s) and removes any access grants the role(s) have on other objects. This function is used during role deletion to clean up all dependencies.

## Definition

```c
void
shdepDropOwned(List *roleids, DropBehavior behavior)
```
## Detailed Description
The shdepDropOwned function scans the pg_shdepend catalog to find all objects that depend on the given roles and handles them according to their dependency type. It performs the following operations:

1. **Ownership Dependencies**: Objects owned by the role are collected for deletion
2. **ACL Dependencies**: Access grants to the role are removed from object ACLs  
3. **Policy Dependencies**: The role is removed from row-level security policies, or the entire policy is deleted if removal fails
4. **Initial ACL Dependencies**: References in pg_init_privs are cleaned up

The function uses a two-phase approach: grants and policy modifications are handled immediately during the scan, while object deletions are deferred and performed in batch using performMultipleDeletions to avoid dependency ordering issues.

## Parameters / Member Variables
- `*roleids`: List of role OIDs to process for owned object deletion
- `behavior`: DropBehavior enum controlling cascade vs restrict semantics for deletions
## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md) - Creates ObjectAddresses collection for batch deletion
  - [table_open](../t/table_open.md)/table_close - Opens pg_shdepend catalog with RowExclusiveLock
  - [systable_beginscan](systable_beginscan.md)/systable_getnext/systable_endscan - Scans pg_shdepend entries
  - [IsPinnedObject](../I/IsPinnedObject.md) - Checks if role is system-critical and cannot be dropped
  - [RemoveRoleFromObjectPolicy](../R/RemoveRoleFromObjectPolicy.md) - Attempts to remove role from RLS policy
  - [RemoveRoleFromObjectACL](../R/RemoveRoleFromObjectACL.md) - Removes role from object's ACL
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md) - Cleans up pg_init_privs entries
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md)/ReleaseDeletionLock - Manages object locking for deletion
  - [systable_recheck_tuple](systable_recheck_tuple.md) - Verifies tuple validity after lock acquisition
  - [add_exact_object_address](../a/add_exact_object_address.md) - Adds object to deletion list
  - [sort_object_addresses](sort_object_addresses.md) - Orders objects for stable deletion sequence
  - [performMultipleDeletions](../p/performMultipleDeletions.md) - Executes batch deletion with dependency resolution
  - [free_object_addresses](../f/free_object_addresses.md) - Cleans up ObjectAddresses structure

- Called from (representative examples):
  - [DropOwnedObjects](../D/DropOwnedObjects.md) (src/backend/commands/user.c:1602)

## Notes and Other Information
- Protected against dropping pinned system objects by checking IsPinnedObject
- Only processes objects in the current database or shared objects (dbid filtering)
- Uses systable_recheck_tuple to handle concurrent modifications during processing
- Sorting objects before deletion provides stable error reporting and may improve performance
- Handles different shared dependency types (OWNER, ACL, POLICY, INITACL) with type-specific logic
- Part of the role management infrastructure, typically called during DROP OWNED BY operations

## Simplified Source

```c
void
shdepDropOwned(List *roleids, DropBehavior behavior)
{
    Relation sdepRel;
    ListCell *cell;
    ObjectAddresses *deleteobjs;

    deleteobjs = new_object_addresses();

    // Open shared dependency catalog with exclusive lock
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    // Process each role to find dependent objects
    foreach(cell, roleids) {
        Oid roleid = lfirst_oid(cell);
        ScanKeyData key[2];
        SysScanDesc scan;
        HeapTuple tuple;

        // Check if role is pinned (system-critical)
        if (IsPinnedObject(AuthIdRelationId, roleid)) {
            ObjectAddress obj;
            obj.classId = AuthIdRelationId;
            obj.objectId = roleid;
            obj.objectSubId = 0;
            ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                           errmsg("cannot drop objects owned by %s because they are "
                                  "required by the database system",
                                  getObjectDescription(&obj, false))));
        }

        // Set up scan keys for this role
        ScanKeyInit(&key[0], Anum_pg_shdepend_refclassid,
                   BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(AuthIdRelationId));
        ScanKeyInit(&key[1], Anum_pg_shdepend_refobjid,
                   BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

        scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, NULL, 2, key);

        while ((tuple = systable_getnext(scan)) != NULL) {
            Form_pg_shdepend sdepForm = (Form_pg_shdepend) GETSTRUCT(tuple);
            ObjectAddress obj;

            // Only process objects in current database or shared objects
            if (sdepForm->dbid != MyDatabaseId && sdepForm->dbid != InvalidOid)
                continue;

            switch (sdepForm->deptype) {
                case SHARED_DEPENDENCY_POLICY:
                    // Try to remove role from policy; if unable, delete policy
                    if (!RemoveRoleFromObjectPolicy(roleid, sdepForm->classid, sdepForm->objid)) {
                        obj.classId = sdepForm->classid;
                        obj.objectId = sdepForm->objid;
                        obj.objectSubId = sdepForm->objsubid;

                        AcquireDeletionLock(&obj, 0);
                        if (systable_recheck_tuple(scan, tuple)) {
                            add_exact_object_address(&obj, deleteobjs);
                        } else {
                            ReleaseDeletionLock(&obj);
                        }
                    }
                    break;

                case SHARED_DEPENDENCY_ACL:
                    // Remove role from ACL (unless it's role membership)
                    if (sdepForm->classid != AuthMemRelationId) {
                        RemoveRoleFromObjectACL(roleid, sdepForm->classid, sdepForm->objid);
                        break;
                    }
                    // Fall through for role membership

                case SHARED_DEPENDENCY_OWNER:
                    // Schedule for deletion if local object or role grant
                    if (sdepForm->dbid == MyDatabaseId || sdepForm->classid == AuthMemRelationId) {
                        obj.classId = sdepForm->classid;
                        obj.objectId = sdepForm->objid;
                        obj.objectSubId = sdepForm->objsubid;

                        AcquireDeletionLock(&obj, 0);
                        if (systable_recheck_tuple(scan, tuple)) {
                            add_exact_object_address(&obj, deleteobjs);
                        } else {
                            ReleaseDeletionLock(&obj);
                        }
                    }
                    break;

                case SHARED_DEPENDENCY_INITACL:
                    // Remove role from initial privileges
                    RemoveRoleFromInitPriv(roleid, sdepForm->classid,
                                         sdepForm->objid, sdepForm->objsubid);
                    break;

                default:
                    elog(ERROR, "unexpected dependency type");
            }
        }
        systable_endscan(scan);
    }

    // Sort objects for stable deletion order and perform batch deletion
    sort_object_addresses(deleteobjs);
    performMultipleDeletions(deleteobjs, behavior, 0);

    table_close(sdepRel, RowExclusiveLock);
    free_object_addresses(deleteobjs);
}
```