# DropObjectById

## Location
[src/backend/catalog/dependency.c:1189-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1189-L1245)

## Overview
DropObjectById is a static utility function that removes an object from its catalog table by OID, used as a low-level deletion mechanism for most PostgreSQL system catalogs when no special processing is required.

## Definition

```c
static void
DropObjectById(const ObjectAddress *object)
```
## Detailed Description
DropObjectById provides a generic mechanism for deleting catalog entries by their object identifier. The function handles the low-level details of catalog tuple deletion, supporting both cached and non-cached lookup strategies. It first attempts to use the system cache if available for the object's catalog, falling back to a sequential scan approach when no cache exists. The function ensures proper locking by opening the target relation with RowExclusiveLock and maintains data consistency by using CatalogTupleDelete for the actual removal operation.

## Parameters / Member Variables
- : Pointer to an ObjectAddress structure containing:
  - : OID of the catalog relation containing the object
  - : OID of the specific object to be deleted
  - : Sub-object identifier (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md): Retrieves cache ID for the object's catalog
  - [table_open](../t/table_open.md): Opens the catalog relation with specified lock
  - [SearchSysCache1](../S/SearchSysCache1.md): Performs cached lookup of the target tuple
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Deletes the catalog tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the cached tuple
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for non-cached lookup
  - [systable_beginscan](../s/systable_beginscan.md): Begins system table scan
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md): Gets OID attribute number
  - [get_object_oid_index](../g/get_object_oid_index.md): Gets OID index for the catalog
  - [systable_getnext](../s/systable_getnext.md): Retrieves next tuple from scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - [table_close](../t/table_close.md): Closes the catalog relation
  - [get_object_class_descr](../g/get_object_class_descr.md): Gets human-readable class description for errors
- Called from:
  - [doDeletion](../d/doDeletion.md): Main deletion orchestration function

## Notes and Other Information
- This function is static and only used internally within the dependency.c module
- It implements a two-path deletion strategy: cached lookup for performance when available, sequential scan as fallback
- The function expects exactly one matching tuple and will error if the object is not found
- Proper error handling includes descriptive messages using get_object_class_descr
- Uses RowExclusiveLock to ensure safe concurrent access during deletion operations

## Simplified Source

```c
static void
DropObjectById(const ObjectAddress *object)
{
    int cacheId;
    Relation rel;
    HeapTuple tup;

    // Get cache info and open catalog relation
    cacheId = get_object_catcache_oid(object->classId);
    rel = table_open(object->classId, RowExclusiveLock);

    // Try cached lookup first for better performance
    if (cacheId >= 0) {
        // Use system cache to find the tuple
        tup = SearchSysCache1(cacheId, ObjectIdGetDatum(object->objectId));
        if (!HeapTupleIsValid(tup))
            elog(ERROR, "cache lookup failed for %s %u",
                 get_object_class_descr(object->classId), object->objectId);

        // Delete the catalog tuple
        CatalogTupleDelete(rel, &tup->t_self);
        ReleaseSysCache(tup);
    }
    else {
        // Fallback: scan catalog table directly
        ScanKeyData skey[1];
        SysScanDesc scan;

        // Set up scan key for object OID
        ScanKeyInit(&skey[0], get_object_attnum_oid(object->classId),
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(object->objectId));

        // Scan for the target tuple
        scan = systable_beginscan(rel, get_object_oid_index(object->classId),
                                  true, NULL, 1, skey);

        tup = systable_getnext(scan);
        if (!HeapTupleIsValid(tup))
            elog(ERROR, "could not find tuple for %s %u",
                 get_object_class_descr(object->classId), object->objectId);

        // Delete the catalog tuple
        CatalogTupleDelete(rel, &tup->t_self);
        systable_endscan(scan);
    }

    table_close(rel, RowExclusiveLock);
}
```