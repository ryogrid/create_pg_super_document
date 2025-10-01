# RemoveAttrDefaultById

## Location
[src/backend/catalog/pg_attrdef.c:274-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L274-L344)

## Overview
RemoveAttrDefaultById removes a pg_attrdef entry specified by its OID and updates the corresponding pg_attribute entry to reflect that no default exists, serving as the core implementation for attribute default removal.

## Definition
```c
void RemoveAttrDefaultById(Oid attrdefId)
```

## Detailed Description
This function performs the low-level removal of an attribute default entry when given the specific OID of the pg_attrdef tuple. It first locates the target tuple in pg_attrdef using the provided OID, extracts the relation ID and attribute number from that tuple, then acquires an exclusive lock on the owning relation for safety. After deleting the pg_attrdef tuple, it updates the corresponding pg_attribute entry to set atthasdef to false, indicating no default exists. The function is designed to be called through the dependency system (via performDeletion) rather than directly, ensuring proper cascade handling and consistency checks are performed.

## Parameters / Member Variables
- `attrdefId`: The OID of the pg_attrdef tuple to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for OID-based lookup
  - [systable_beginscan](../s/systable_beginscan.md): Begins scan of pg_attrdef using OID index
  - [systable_getnext](../s/systable_getnext.md): Retrieves the target tuple from scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - [relation_open](../r/relation_open.md): Opens the owning relation with exclusive lock
  - [relation_close](../r/relation_close.md): Closes relation while maintaining lock until transaction end
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Deletes the pg_attrdef tuple
  - SearchSysCacheCopy2: Finds corresponding pg_attribute entry
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates pg_attribute to clear atthasdef flag
  - [table_open](../t/table_open.md)/table_close: Opens and closes system catalog tables

- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md): Called by dependency system during object deletion cascades

## Notes and Other Information
The function maintains careful locking protocol - it acquires exclusive locks on both the pg_attrdef catalog and the owning relation to prevent concurrent modifications during the deletion process. The exclusive lock on the owning relation is held until transaction end to ensure consistency. The function includes error handling for cases where the specified attrdef OID doesn't exist or the corresponding attribute cannot be found. The update to pg_attribute will automatically trigger relcache rebuilds, ensuring that cached relation information reflects the removal of the default. This function should not be called directly but rather through the dependency management system to ensure proper cascade behavior and dependency checking.

## Simplified Source

```c
void
RemoveAttrDefaultById(Oid attrdefId)
{
    Relation attrdef_rel, attr_rel, myrel;
    ScanKeyData scankeys[1];
    SysScanDesc scan;
    HeapTuple tuple;
    Oid myrelid;
    AttrNumber myattnum;

    // Open pg_attrdef catalog with lock
    attrdef_rel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    // Find the specific attrdef tuple by OID
    ScanKeyInit(&scankeys[0], Anum_pg_attrdef_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(attrdefId));
    scan = systable_beginscan(attrdef_rel, AttrDefaultOidIndexId, true, NULL, 1, scankeys);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "could not find tuple for attrdef %u", attrdefId);

    // Extract relation and attribute info
    myrelid = ((Form_pg_attrdef) GETSTRUCT(tuple))->adrelid;
    myattnum = ((Form_pg_attrdef) GETSTRUCT(tuple))->adnum;

    // Lock the owning relation
    myrel = relation_open(myrelid, AccessExclusiveLock);

    // Delete the attrdef tuple
    CatalogTupleDelete(attrdef_rel, &tuple->t_self);
    systable_endscan(scan);
    table_close(attrdef_rel, RowExclusiveLock);

    // Update pg_attribute to clear atthasdef flag
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(myrelid), Int16GetDatum(myattnum));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for attribute %d of relation %u", myattnum, myrelid);

    ((Form_pg_attribute) GETSTRUCT(tuple))->atthasdef = false;
    CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

    // Clean up and maintain locks until transaction end
    table_close(attr_rel, RowExclusiveLock);
    relation_close(myrel, NoLock);
}
```