# pg_class_aclmask_ext

## Location
[src/backend/catalog/aclchk.c:3339-3468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3339-L3468)

## Overview
An extended version of  that provides comprehensive privilege checking for table/relation objects with additional support for missing object detection and enhanced permission logic.

## Definition


## Detailed Description
This function implements the core logic for checking user privileges on PostgreSQL relations (tables, views, sequences, etc.). It performs several layers of access control checking:

1. **Relation Validation**: Retrieves the relation's metadata from pg_class system catalog
2. **System Catalog Protection**: Prevents non-superusers from modifying system catalogs (except views)
3. **Superuser Bypass**: Allows superusers to bypass most permission checks
4. **ACL Processing**: Retrieves and processes the relation's access control list
5. **Default ACL Handling**: Applies appropriate default permissions when no explicit ACL exists
6. **Special Role Privileges**: Grants additional permissions based on predefined roles like pg_read_all_data, pg_write_all_data, and pg_maintain

The function supports graceful handling of missing relations through the  parameter, allowing callers to distinguish between insufficient privileges and non-existent objects.

## Parameters / Member Variables
- : The object identifier (OID) of the relation to check permissions for
- : The OID of the role whose permissions are being checked
- : Bitmask specifying which permissions to check (ACL_SELECT, ACL_INSERT, ACL_UPDATE, ACL_DELETE, ACL_TRUNCATE, ACL_USAGE, ACL_MAINTAIN)
- : Specifies how to combine multiple ACL entries (ACLMASK_ALL or ACLMASK_ANY)
- : Optional output parameter; if provided, set to true when the relation doesn't exist instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - superuser_arg
  - [IsSystemClass](../I/IsSystemClass.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - has_privs_of_role
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [pg_class_aclmask](pg_class_aclmask.md)
  - [pg_class_aclcheck_ext](pg_class_aclcheck_ext.md)
  - InternalDefaultACL

## Notes and Other Information
- This is a static (internal) function, not directly accessible outside aclchk.c
- Handles different relation types (tables, sequences, views) with appropriate default ACLs
- Implements PostgreSQL's predefined role system (pg_read_all_data, pg_write_all_data, pg_maintain)
- System catalog protection ensures only superusers can modify core PostgreSQL metadata
- The function performs efficient caching through the system cache mechanism
- Special handling for sequences vs. tables when determining default permissions
- Memory management includes proper cleanup of detoasted ACL data