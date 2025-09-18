# object_aclmask_ext

## Location
src/backend/catalog/aclchk.c: 3112 - 3203

## Overview
The core implementation function for examining user privileges on database objects, providing comprehensive ACL checking with optional missing object handling and snapshot control.

## Definition
```c
static AclMode object_aclmask_ext(Oid classid, Oid objectid, Oid roleid, AclMode mask, AclMaskHow how, bool *is_missing)
```

## Detailed Description
The `object_aclmask_ext` function is the comprehensive implementation for PostgreSQL's object-level access control checking. It handles the complete process of retrieving, parsing, and evaluating Access Control Lists (ACLs) for database objects.

The function first handles special cases by delegating namespace and type objects to their specialized functions (`pg_namespace_aclmask_ext` and `pg_type_aclmask_ext`). It includes assertions to ensure that relations and large objects are not processed here, as they have their own dedicated handlers.

For superusers, the function immediately grants all requested permissions without further checking. For regular users, it retrieves the object's ACL from the appropriate system catalog, handles cases where no explicit ACL exists by building a default ACL, and then evaluates the permissions using the core `aclmask` function.

The function provides robust error handling with an optional `is_missing` parameter that allows callers to handle non-existent objects gracefully by returning zero permissions instead of throwing an error.

## Parameters / Member Variables
- `classid`: The OID of the system catalog relation containing the object
- `objectid`: The OID of the specific database object being checked
- `roleid`: The OID of the role whose permissions are being examined
- `mask`: The access permissions being requested (AclMode bitmask)
- `how`: Specifies the method for ACL evaluation (AclMaskHow enum)
- `is_missing`: Optional output parameter; if not NULL, set to true if object doesn't exist (instead of throwing error)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_namespace_aclmask_ext](../p/pg_namespace_aclmask_ext.md)
  - [pg_type_aclmask_ext](../p/pg_type_aclmask_ext.md)
  - superuser_arg
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)
  - [get_object_class_descr](../g/get_object_class_descr.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [get_object_attnum_owner](../g/get_object_attnum_owner.md)
  - [get_object_attnum_acl](../g/get_object_attnum_acl.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [get_object_type](../g/get_object_type.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [object_aclmask](object_aclmask.md)
  - [object_aclcheck_ext](object_aclcheck_ext.md)

## Notes and Other Information
- This is a static function internal to the aclchk.c module
- Includes explicit assertions preventing misuse with relations and large objects
- Handles ACL detoasting automatically and cleans up memory properly
- Provides graceful handling of missing objects when `is_missing` parameter is provided
- Superuser bypass occurs early for performance optimization
- The function manages system cache interactions properly with SearchSysCache1/ReleaseSysCache pairs
- Default ACLs are generated on-the-fly when objects have no explicit ACL defined