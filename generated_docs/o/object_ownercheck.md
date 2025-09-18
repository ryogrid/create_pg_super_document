# object_ownercheck

## Location
src/backend/catalog/aclchk.c: 4147 - 4227

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
  - superuser_arg
  - get_object_catcache_oid
  - get_object_class_descr
  - SysCacheGetAttrNotNull
  - DatumGetObjectId
  - get_object_attnum_owner
  - get_object_attnum_oid
  - systable_beginscan
  - get_object_oid_index
  - systable_getnext
  - heap_getattr
  - has_privs_of_role
- Called from (representative examples):
  - check_object_ownership
  - ATSimplePermissions
  - AlterFunction
  - DropTableSpace
  - DefineType
  - vacuum_is_permitted_for_relation

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4147-4227
- Handles special case for large objects by mapping LargeObjectRelationId to LargeObjectMetadataRelationId
- Uses syscache when available for better performance, falls back to direct catalog scanning
- Superusers automatically pass all ownership checks
- Widely used throughout PostgreSQL for DDL operations requiring object ownership
- Returns true if the role owns the object or has equivalent privileges, false otherwise
- Critical function for PostgreSQL's security model and privilege system