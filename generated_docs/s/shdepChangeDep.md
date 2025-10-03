# shdepChangeDep

## Location
[src/backend/catalog/pg_shdepend.c:206-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L206-L315)

## Overview
Internal function that updates shared dependency records when the referenced object changes, handling ownership and tablespace dependency modifications.

## Definition

```c
static void
shdepChangeDep(Relation sdepRel,
			   Oid classid, Oid objid, int32 objsubid,
			   Oid refclassid, Oid refobjid,
			   SharedDependencyType deptype)
```
## Detailed Description
This is a core internal function that handles updating pg_shdepend entries when a referenced shared object changes (such as during owner or tablespace changes). It performs intelligent dependency management by: 1) searching for existing dependency entries, 2) handling pinned objects appropriately (not creating dependencies for them), 3) updating existing entries or creating new ones as needed, and 4) cleaning up when dependencies are no longer required. The function ensures there is only one entry per dependent object and dependency type.

## Parameters / Member Variables
- `sdepRel`: Already opened pg_shdepend relation with appropriate lock
- `classid`: OID of the catalog containing the dependent object
- `objid`: OID of the dependent object
- `objsubid`: Sub-object ID (typically 0 for most objects)
- `refclassid`: OID of the catalog containing the new referenced object
- `refobjid`: OID of the new referenced object
- `deptype`: Type of shared dependency (SHARED_DEPENDENCY_OWNER or SHARED_DEPENDENCY_TABLESPACE)
## Dependencies
- Functions called/Symbols referenced:
  - [classIdGetDbId](../c/classIdGetDbId.md)
  - [shdepLockAndCheckObject](shdepLockAndCheckObject.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](systable_beginscan.md)
  - [systable_getnext](systable_getnext.md)
  - [systable_endscan](systable_endscan.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - [changeDependencyOnTablespace](../c/changeDependencyOnTablespace.md)

## Notes and Other Information
- Static function - internal use only within pg_shdepend.c
- Enforces single dependency entry constraint - errors if multiple matches found
- Handles three scenarios: update existing entry, delete entry (for pinned objects), or insert new entry
- Uses heap_copytuple to make modifiable copies of catalog tuples
- Properly locks referenced objects to prevent them from being dropped during the operation
- Located in src/backend/catalog/pg_shdepend.c:206-315

## Simplified Source

```c
static void
shdepChangeDep(Relation sdepRel,
               Oid classid, Oid objid, int32 objsubid,
               Oid refclassid, Oid refobjid,
               SharedDependencyType deptype)
{
    Oid         dbid = classIdGetDbId(classid);
    HeapTuple   oldtup = NULL;
    HeapTuple   scantup;
    ScanKeyData key[4];
    SysScanDesc scan;

    // Lock the new referenced object to prevent it from being dropped
    shdepLockAndCheckObject(refclassid, refobjid);

    // Setup scan keys to find existing dependency entry
    ScanKeyInit(&key[0], Anum_pg_shdepend_dbid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(dbid));
    ScanKeyInit(&key[1], Anum_pg_shdepend_classid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classid));
    ScanKeyInit(&key[2], Anum_pg_shdepend_objid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));
    ScanKeyInit(&key[3], Anum_pg_shdepend_objsubid,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true,
                              NULL, 4, key);

    // Find existing entry of the specified dependency type
    while ((scantup = systable_getnext(scan)) != NULL)
    {
        if (((Form_pg_shdepend) GETSTRUCT(scantup))->deptype != deptype)
            continue;

        // Should only be one entry
        if (oldtup)
            elog(ERROR, "multiple pg_shdepend entries for object %u/%u/%d deptype %c",
                 classid, objid, objsubid, deptype);
        oldtup = heap_copytuple(scantup);
    }

    systable_endscan(scan);

    if (IsPinnedObject(refclassid, refobjid))
    {
        // Delete existing entry for pinned objects (no dependency needed)
        if (oldtup)
            CatalogTupleDelete(sdepRel, &oldtup->t_self);
    }
    else if (oldtup)
    {
        // Update existing entry
        Form_pg_shdepend shForm = (Form_pg_shdepend) GETSTRUCT(oldtup);
        shForm->refclassid = refclassid;
        shForm->refobjid = refobjid;
        CatalogTupleUpdate(sdepRel, &oldtup->t_self, oldtup);
    }
    else
    {
        // Insert new entry
        Datum       values[Natts_pg_shdepend];
        bool        nulls[Natts_pg_shdepend];

        memset(nulls, false, sizeof(nulls));

        values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(dbid);
        values[Anum_pg_shdepend_classid - 1] = ObjectIdGetDatum(classid);
        values[Anum_pg_shdepend_objid - 1] = ObjectIdGetDatum(objid);
        values[Anum_pg_shdepend_objsubid - 1] = Int32GetDatum(objsubid);
        values[Anum_pg_shdepend_refclassid - 1] = ObjectIdGetDatum(refclassid);
        values[Anum_pg_shdepend_refobjid - 1] = ObjectIdGetDatum(refobjid);
        values[Anum_pg_shdepend_deptype - 1] = CharGetDatum(deptype);

        oldtup = heap_form_tuple(RelationGetDescr(sdepRel), values, nulls);
        CatalogTupleInsert(sdepRel, oldtup);
    }

    if (oldtup)
        heap_freetuple(oldtup);
}
```