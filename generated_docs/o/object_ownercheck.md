# object_ownercheck

## Location
[src/backend/catalog/aclchk.c:4147-4227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4147-L4227)

## Overview
Performs a generic ownership check for any PostgreSQL database object, verifying whether a given role owns or has privileges equivalent to the owner of the specified object.

## Definition
```c
bool object_ownercheck(Oid classid, Oid objectid, Oid roleid)
```

## Detailed Description
This function provides a centralized mechanism for checking object ownership across all PostgreSQL system catalogs. It first checks if the role is a superuser (which bypasses all permission checks), then retrieves the owner OID of the specified object from the appropriate system catalog. The function handles both objects that have dedicated syscache entries and those that require direct catalog table scanning. Finally, it uses `has_privs_of_role` to determine if the specified role has ownership privileges over the object.

## Parameters / Member Variables
- `classid`: The OID of the system catalog relation containing the object (e.g., RelationRelationId for tables)
- `objectid`: The OID of the specific object whose ownership is being checked
- `roleid`: The OID of the role whose ownership privileges are being verified

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)
  - [get_object_class_descr](../g/get_object_class_descr.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [get_object_attnum_owner](../g/get_object_attnum_owner.md)
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [get_object_oid_index](../g/get_object_oid_index.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
- Called from (representative examples):
  - [check_object_ownership](../c/check_object_ownership.md)
  - [ATSimplePermissions](../A/ATSimplePermissions.md)
  - [AlterFunction](../A/AlterFunction.md)
  - [DropTableSpace](../D/DropTableSpace.md)
  - [DefineType](../D/DefineType.md)
  - [vacuum_is_permitted_for_relation](../v/vacuum_is_permitted_for_relation.md)

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4147-4227
- Handles special case for large objects by mapping LargeObjectRelationId to LargeObjectMetadataRelationId
- Uses syscache when available for better performance, falls back to direct catalog scanning
- Superusers automatically pass all ownership checks
- Widely used throughout PostgreSQL for DDL operations requiring object ownership
- Returns true if the role owns the object or has equivalent privileges, false otherwise
- Critical function for PostgreSQL's security model and privilege system

## Simplified Source

```c
bool
object_ownercheck(Oid classid, Oid objectid, Oid roleid)
{
    int cacheid;
    Oid ownerId;

    // Superusers bypass all permission checking
    if (superuser_arg(roleid))
        return true;

    // For large objects, use the metadata catalog
    if (classid == LargeObjectRelationId)
        classid = LargeObjectMetadataRelationId;

    cacheid = get_object_catcache_oid(classid);
    if (cacheid != -1)
    {
        // Get the object's tuple from the syscache
        HeapTuple tuple;

        tuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objectid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, "%s with OID %u does not exist",
                   get_object_class_descr(classid), objectid);

        ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(cacheid, tuple,
                                                         get_object_attnum_owner(classid)));
        ReleaseSysCache(tuple);
    }
    else
    {
        // For catalogs without an appropriate syscache
        Relation rel;
        ScanKeyData entry[1];
        SysScanDesc scan;
        HeapTuple tuple;
        bool isnull;

        rel = table_open(classid, AccessShareLock);

        ScanKeyInit(&entry[0],
                   get_object_attnum_oid(classid),
                   BTEqualStrategyNumber, F_OIDEQ,
                   ObjectIdGetDatum(objectid));

        scan = systable_beginscan(rel,
                                 get_object_oid_index(classid), true,
                                 NULL, 1, entry);

        tuple = systable_getnext(scan);
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, "%s with OID %u does not exist",
                   get_object_class_descr(classid), objectid);

        ownerId = DatumGetObjectId(heap_getattr(tuple,
                                               get_object_attnum_owner(classid),
                                               RelationGetDescr(rel),
                                               &isnull));
        Assert(!isnull);

        systable_endscan(scan);
        table_close(rel, AccessShareLock);
    }

    return has_privs_of_role(roleid, ownerId);
}
```