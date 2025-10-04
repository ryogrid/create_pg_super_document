# changeDependenciesOn

## Location
[src/backend/catalog/pg_depend.c:622-709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L622-L709)

## Overview
Adjusts all dependency records to redirect them from an old referenced object to a new referenced object of the same type, effectively transferring all incoming dependencies.

## Definition
```c
long changeDependenciesOn(Oid refClassId, Oid oldRefObjectId, Oid newRefObjectId)
```

## Detailed Description
This function performs a bulk transfer of dependency targets by updating all dependency records where a specific object acts as the dependee (referenced object). It changes the target of all dependencies from the old object to the new object while preserving all other aspects of the dependency relationships, including the referencing objects, dependency types, and subobject references.

The function includes important safety checks for pinned objects:
- Refuses to operate if the old referenced object is pinned, raising an error since system objects shouldn't have dependency records pointing to them
- If the new referenced object is pinned, deletes the dependency records entirely rather than updating them
- For normal cases, updates the dependency records to point to the new referenced object

Unlike changeDependenciesOf, this function uses DependReferenceIndexId for efficient scanning based on the referenced object rather than the referencing object.

## Parameters / Member Variables
- `refClassId`: OID of the catalog table containing both old and new referenced objects  
- `oldRefObjectId`: OID of the existing referenced object whose incoming dependencies will be transferred
- `newRefObjectId`: OID of the new referenced object that will inherit all incoming dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [isObjectPinned](../i/isObjectPinned.md)
  - ereport
  - [getObjectDescription](../g/getObjectDescription.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - GETSTRUCT
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [SysScanDesc](../S/SysScanDesc.md)
  - Form_pg_depend

- Called from (representative examples):
  - [index_concurrently_swap](../i/index_concurrently_swap.md) (src/backend/catalog/index.c:1789,1792)

## Notes and Other Information
- Explicitly prevents operations on pinned old objects, raising a descriptive error about system object dependencies
- Uses DependReferenceIndexId for efficient lookup of all records referencing the old object
- Handles the transition to pinned new objects by removing dependency records rather than updating them
- Primarily used in concurrent index operations alongside changeDependenciesOf to completely swap dependency relationships between objects
- The function ensures referential integrity by either updating or removing dependency records appropriately based on the pinned status of the new object
- Returns the count of affected records, providing feedback on the scope of the dependency transfer operation

## Simplified Source
```c
long changeDependenciesOn(Oid refClassId, Oid oldRefObjectId, Oid newRefObjectId) {
    long count = 0;
    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;
    ObjectAddress objAddr;
    bool newIsPinned;

    // Open dependency relation for modification
    depRel = table_open(DependRelationId, RowExclusiveLock);

    // Check if old object is pinned (system object)
    objAddr.classId = refClassId;
    objAddr.objectId = oldRefObjectId;
    objAddr.objectSubId = 0;

    if (isObjectPinned(&objAddr))
        ereport(ERROR, "cannot remove dependency on system object");

    // Check if new object is pinned
    objAddr.objectId = newRefObjectId;
    newIsPinned = isObjectPinned(&objAddr);

    // Set up scan keys to find all dependencies on old object
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(refClassId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(oldRefObjectId));

    // Scan for dependency records referencing the old object
    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        if (newIsPinned) {
            // If new object is pinned, delete the dependency record
            CatalogTupleDelete(depRel, &tup->t_self);
        } else {
            // Update dependency to point to new object
            Form_pg_depend depform;
            tup = heap_copytuple(tup);
            depform = (Form_pg_depend) GETSTRUCT(tup);
            depform->refobjid = newRefObjectId;
            CatalogTupleUpdate(depRel, &tup->t_self, tup);
            heap_freetuple(tup);
        }
        count++;
    }

    systable_endscan(scan);
    table_close(depRel, RowExclusiveLock);
    return count;
}
```