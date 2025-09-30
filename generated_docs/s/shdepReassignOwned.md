# shdepReassignOwned

## Location
[src/backend/catalog/pg_shdepend.c:1530-1646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1530-L1646)

## Overview
Reassigns ownership of all objects owned by the specified role(s) to a new role. Unlike shdepDropOwned, this function transfers ownership rather than deleting objects and does not modify grants.

## Definition
```c
void shdepReassignOwned(List *roleids, Oid newrole)
```

## Detailed Description
The shdepReassignOwned function scans the pg_shdepend catalog to find all objects that have ownership dependencies on the given roles and transfers ownership to the specified new role. It processes different types of shared dependencies:

1. **SHARED_DEPENDENCY_OWNER**: Calls shdepReassignOwned_Owner to transfer object ownership
2. **SHARED_DEPENDENCY_INITACL**: Calls shdepReassignOwned_InitAcl to update initial privileges
3. **Other dependency types**: ACL, POLICY, and TABLESPACE dependencies are ignored as they don't involve ownership

The function includes memory management optimization by creating short-lived memory contexts for each object processed, preventing memory leaks when processing large numbers of objects. Each iteration calls CommandCounterIncrement to ensure changes are visible to subsequent operations.

## Parameters / Member Variables
- `roleids`: List of role OIDs whose owned objects should be reassigned 
- `newrole`: OID of the role that will become the new owner of the objects

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close - Opens pg_shdepend catalog with RowExclusiveLock
  - [IsPinnedObject](../I/IsPinnedObject.md) - Checks if role is system-critical and cannot be processed
  - [getObjectDescription](../g/getObjectDescription.md) - Generates error message descriptions
  - [systable_beginscan](systable_beginscan.md)/systable_getnext/systable_endscan - Scans pg_shdepend entries
  - AllocSetContextCreate/MemoryContextDelete - Manages memory contexts for leak prevention
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) - Switches between memory contexts
  - [shdepReassignOwned_Owner](shdepReassignOwned_Owner.md) - Handles ownership reassignment for OWNER dependencies
  - [shdepReassignOwned_InitAcl](shdepReassignOwned_InitAcl.md) - Handles initial ACL reassignment for INITACL dependencies
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) - Ensures changes are visible to subsequent operations

- Called from (representative examples):
  - [ReassignOwnedObjects](../R/ReassignOwnedObjects.md) (src/backend/commands/user.c:1641)

## Notes and Other Information
- Protected against reassigning ownership from pinned system roles
- Only processes objects in the current database or shared objects (dbid filtering)
- Uses memory context management to prevent memory leaks during bulk operations
- Calls CommandCounterIncrement after each object to ensure transaction visibility
- Part of the role management infrastructure, typically called during REASSIGN OWNED operations
- Does not modify grants or ACLs, only ownership relationships
- Delegates actual ownership changes to specialized helper functions for different dependency types

## Simplified Source

```c
void shdepReassignOwned(List *roleids, Oid newrole) {
    Relation sdepRel;
    ListCell *cell;

    // Open shared dependency catalog with exclusive lock
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    foreach(cell, roleids) {
        Oid roleid = lfirst_oid(cell);

        // Refuse to work on system-critical pinned roles
        if (IsPinnedObject(AuthIdRelationId, roleid)) {
            ereport(ERROR,
                    (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                     errmsg("cannot reassign ownership of objects owned by %s because they are required by the database system",
                            getObjectDescription(&obj, false))));
        }

        // Set up scan to find all dependencies for this role
        ScanKeyData key[2];
        ScanKeyInit(&key[0], Anum_pg_shdepend_refclassid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(AuthIdRelationId));
        ScanKeyInit(&key[1], Anum_pg_shdepend_refobjid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(roleid));

        SysScanDesc scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId,
                                            true, NULL, 2, key);

        // Process each dependency entry
        HeapTuple tuple;
        while ((tuple = systable_getnext(scan)) != NULL) {
            Form_pg_shdepend sdepForm = (Form_pg_shdepend) GETSTRUCT(tuple);

            // Only process current database or shared objects
            if (sdepForm->dbid != MyDatabaseId && sdepForm->dbid != InvalidOid)
                continue;

            // Use temporary memory context to prevent leaks
            MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,
                                                    "shdepReassignOwned",
                                                    ALLOCSET_DEFAULT_SIZES);
            MemoryContext oldcxt = MemoryContextSwitchTo(cxt);

            // Handle different dependency types
            switch (sdepForm->deptype) {
                case SHARED_DEPENDENCY_OWNER:
                    shdepReassignOwned_Owner(sdepForm, newrole);
                    break;
                case SHARED_DEPENDENCY_INITACL:
                    shdepReassignOwned_InitAcl(sdepForm, roleid, newrole);
                    break;
                case SHARED_DEPENDENCY_ACL:
                case SHARED_DEPENDENCY_POLICY:
                case SHARED_DEPENDENCY_TABLESPACE:
                    // Nothing to do for these entry types
                    break;
            }

            // Clean up memory and ensure changes are visible
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(cxt);
            CommandCounterIncrement();
        }

        systable_endscan(scan);
    }

    table_close(sdepRel, RowExclusiveLock);
}
```