# pg_namespace_aclmask_ext

## Location
src/backend/catalog/aclchk.c: 3665 - 3766

## Overview
This is an internal function that examines a user's privileges for a namespace (schema), with support for handling missing objects gracefully through an optional is_missing parameter.

## Definition


## Detailed Description
The function performs comprehensive privilege checking for PostgreSQL namespaces (schemas). It handles several special cases including superuser bypass, temporary namespace permissions, and role-based access for pg_read_all_data/pg_write_all_data roles. The function retrieves the Access Control List (ACL) from pg_namespace system catalog and evaluates permissions against it. If the is_missing parameter is provided, the function can return gracefully when the namespace doesn't exist rather than throwing an error.

## Parameters / Member Variables
- : The Object ID of the namespace to check permissions for
- : The Object ID of the role whose permissions are being checked  
- : The permission mask specifying which privileges to check (e.g., ACL_USAGE, ACL_CREATE)
- : Enumeration specifying how to combine privileges (AclMaskHow type)
- : Optional pointer to bool that gets set to true if the namespace doesn't exist (allows graceful handling)

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - [isTempNamespace](../i/isTempNamespace.md)  
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - has_privs_of_role
- Called from (representative examples):
  - InternalDefaultACL
  - [object_aclmask_ext](../o/object_aclmask_ext.md)

## Notes and Other Information
- Superusers automatically bypass all permission checks
- Special handling for temporary namespaces: grants all standard rights if user has CREATE TEMP on database, otherwise only USAGE
- For missing namespaces, can either return 0 permissions (if is_missing provided) or throw ERRCODE_UNDEFINED_SCHEMA error
- Automatically grants ACL_USAGE to members of pg_read_all_data or pg_write_all_data roles if not already granted
- Function is static (internal to aclchk.c) and used primarily by the broader object permission checking infrastructure