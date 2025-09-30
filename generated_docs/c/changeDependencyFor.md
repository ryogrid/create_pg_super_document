# changeDependencyFor

## Location
[src/backend/catalog/pg_depend.c:458-565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L458-L565)

## Overview
Adjusts dependency records to point from a specific referencing object to a different referenced object of the same type, handling special cases for pinned objects.

## Definition
```c
long changeDependencyFor(Oid classId, Oid objectId, Oid refClassId, Oid oldRefObjectId, Oid newRefObjectId)
```

## Detailed Description
This function modifies existing dependency records to redirect them from one referenced object to another while maintaining the same referencing object. It's particularly useful in scenarios like namespace changes, object renames, or when objects are replaced with equivalent ones.

The function handles several special cases involving pinned objects (system objects that cannot be dropped):
- If both old and new objects are pinned, no action is needed (returns 1 for success)  
- If only the old object is pinned, creates a new normal dependency record for the new object
- If only the new object is pinned, deletes the existing dependency record
- For normal cases, updates the existing records to point to the new object

The function processes all matching dependency records, including those with subobject references, ensuring complete redirection of dependencies.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing the referencing object
- `objectId`: OID of the referencing object whose dependencies need to be updated
- `refClassId`: OID of the catalog table containing both old and new referenced objects
- `oldRefObjectId`: OID of the current referenced object to be replaced
- `newRefObjectId`: OID of the new referenced object to point dependencies to

## Dependencies
- Functions called/Symbols referenced:
  - [isObjectPinned](../i/isObjectPinned.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - GETSTRUCT
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [SysScanDesc](../S/SysScanDesc.md)
  - Form_pg_depend
  - DEPENDENCY_NORMAL

- Called from (representative examples):
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (src/backend/commands/alter.c:811)
  - [swap_relation_files](../s/swap_relation_files.md) (src/backend/commands/cluster.c:1275,1283)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md) (src/backend/commands/extension.c:2971)
  - [AlterFunction](../A/AlterFunction.md) (src/backend/commands/functioncmds.c:1449)
  - [AlterRelationNamespaceInternal](../A/AlterRelationNamespaceInternal.md) (src/backend/commands/tablecmds.c:17365)

## Notes and Other Information
- Returns the number of records updated; zero indicates a potential problem since at least one record should normally exist
- Assumes NORMAL dependency type when creating new records for previously pinned objects
- Handles all subobject references automatically without requiring explicit objsubid parameters  
- Used extensively in DDL operations that change object relationships, such as ALTER ... SET SCHEMA commands
- The function's logic ensures that the dependency system remains consistent even when dealing with the complexities of pinned vs. unpinned objects

## Simplified Source

```c
long changeDependencyFor(Oid classId, Oid objectId, Oid refClassId,
                        Oid oldRefObjectId, Oid newRefObjectId) {
    long count = 0;
    bool oldIsPinned, newIsPinned;

    // Check if old or new objects are pinned (system objects)
    ObjectAddress objAddr = {refClassId, oldRefObjectId, 0};
    oldIsPinned = isObjectPinned(&objAddr);

    objAddr.objectId = newRefObjectId;
    newIsPinned = isObjectPinned(&objAddr);

    if (oldIsPinned) {
        if (newIsPinned)
            return 1;  // Both pinned, nothing to do

        // Old was pinned, new isn't - create new dependency record
        ObjectAddress depAddr = {classId, objectId, 0};
        recordDependencyOn(&depAddr, &objAddr, DEPENDENCY_NORMAL);
        return 1;
    }

    // Search and update existing dependency records
    Relation depRel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyData key[2];
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(objectId));

    SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId,
                                         true, NULL, 2, key);

    HeapTuple tup;
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

        if (depform->refclassid == refClassId &&
            depform->refobjid == oldRefObjectId) {

            if (newIsPinned) {
                // New object is pinned - delete dependency record
                CatalogTupleDelete(depRel, &tup->t_self);
            } else {
                // Update dependency to point to new object
                tup = heap_copytuple(tup);
                depform = (Form_pg_depend) GETSTRUCT(tup);
                depform->refobjid = newRefObjectId;
                CatalogTupleUpdate(depRel, &tup->t_self, tup);
                heap_freetuple(tup);
            }
            count++;
        }
    }

    systable_endscan(scan);
    table_close(depRel, RowExclusiveLock);

    return count;
}
```