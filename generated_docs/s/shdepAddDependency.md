# shdepAddDependency

## Location
[src/backend/catalog/pg_shdepend.c:1069-1123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1069-L1123)

## Overview
Internal workhorse function for inserting shared dependency records into the pg_shdepend catalog table, ensuring proper dependency tracking between database objects and shared objects like roles and tablespaces.

## Definition

```c
static void
shdepAddDependency(Relation sdepRel,
				   Oid classId, Oid objectId, int32 objsubId,
				   Oid refclassId, Oid refobjId,
				   SharedDependencyType deptype)
```
## Detailed Description
This function serves as the core implementation for recording shared dependencies in PostgreSQL's dependency tracking system. It performs the actual insertion of a dependency record into the pg_shdepend catalog table, which tracks dependencies between regular database objects and cluster-wide shared objects such as roles, tablespaces, and databases.

The function first locks the referenced object to prevent it from being dropped while the dependency is being recorded, then constructs a new tuple with the dependency information and inserts it into the catalog table. This ensures referential integrity and prevents orphaned objects.

## Parameters / Member Variables
- : Open pg_shdepend relation with appropriate locks held by caller
- : OID of the catalog table that contains the dependent object
- : OID of the dependent object itself
- : Sub-object identifier for the dependent object (0 if not applicable)
- : OID of the catalog table containing the referenced shared object
- : OID of the referenced shared object
- : Type of dependency relationship (SharedDependencyType enum value)

## Dependencies
- Functions called/Symbols referenced:
  - [shdepLockAndCheckObject](shdepLockAndCheckObject.md)
  - [classIdGetDbId](../c/classIdGetDbId.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - [CharGetDatum](../C/CharGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [recordSharedDependencyOn](../r/recordSharedDependencyOn.md)
  - [updateAclDependenciesWorker](../u/updateAclDependenciesWorker.md)

## Notes and Other Information
- This is a static internal function, not directly accessible outside pg_shdepend.c
- The function assumes the caller has already opened and locked the pg_shdepend relation appropriately
- Locking the referenced object prevents race conditions with DROP operations
- The function uses the standard PostgreSQL tuple construction and catalog insertion patterns
- Memory cleanup is properly handled by freeing the constructed tuple after insertion

## Simplified Source
```c
static void shdepAddDependency(Relation sdepRel,
                              Oid classId, Oid objectId, int32 objsubId,
                              Oid refclassId, Oid refobjId,
                              SharedDependencyType deptype)
{
    HeapTuple tup;
    Datum values[Natts_pg_shdepend];
    bool nulls[Natts_pg_shdepend];

    // Lock referenced object to prevent concurrent drops
    shdepLockAndCheckObject(refclassId, refobjId);

    // Initialize nulls array
    memset(nulls, false, sizeof(nulls));

    // Fill in all tuple values for the dependency record
    values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(classIdGetDbId(classId));
    values[Anum_pg_shdepend_classid - 1] = ObjectIdGetDatum(classId);
    values[Anum_pg_shdepend_objid - 1] = ObjectIdGetDatum(objectId);
    values[Anum_pg_shdepend_objsubid - 1] = Int32GetDatum(objsubId);
    values[Anum_pg_shdepend_refclassid - 1] = ObjectIdGetDatum(refclassId);
    values[Anum_pg_shdepend_refobjid - 1] = ObjectIdGetDatum(refobjId);
    values[Anum_pg_shdepend_deptype - 1] = CharGetDatum(deptype);

    // Create tuple and insert into catalog
    tup = heap_form_tuple(sdepRel->rd_att, values, nulls);
    CatalogTupleInsert(sdepRel, tup);

    // Clean up allocated tuple
    heap_freetuple(tup);
}
```