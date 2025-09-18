# pg_type_aclmask_ext

## Location
[src/backend/catalog/aclchk.c:3767-3892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3767-L3892)

## Overview
This internal function examines a user's privileges for a PostgreSQL data type, with support for graceful handling of missing types through an optional is_missing parameter.

## Definition
static AclMode pg_type_aclmask_ext(Oid type_oid, Oid roleid, AclMode mask, AclMaskHow how, bool *is_missing)

## Detailed Description
The function performs permission checking for PostgreSQL data types with special handling for various type categories. It includes logic for array types (which defer to their element types), multirange types (which defer to their associated range types), and standard data types. The function retrieves the Access Control List (ACL) from the pg_type system catalog and evaluates permissions against it. Like other _ext variants, it can handle missing objects gracefully when the is_missing parameter is provided.

## Parameters / Member Variables
- type_oid: The Object ID of the data type to check permissions for
- roleid: The Object ID of the role whose permissions are being checked
- mask: The permission mask specifying which privileges to check 
- how: Enumeration specifying how to combine privileges (AclMaskHow type)
- is_missing: Optional pointer to bool that gets set to true if the type does not exist (enables graceful error handling)

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - IsTrueArrayType
  - [get_multirange_range](../g/get_multirange_range.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [object_aclmask_ext](../o/object_aclmask_ext.md)

## Notes and Other Information
- Superusers automatically bypass all permission checks
- Array types delegate permission checks to their element types rather than managing their own permissions
- Multirange types delegate permission checks to their associated range types
- The function handles the delegation chain: multirange → range → (possibly array element)
- For missing types, can either return 0 permissions (if is_missing provided) or throw ERRCODE_UNDEFINED_OBJECT error
- Function is static (internal to aclchk.c) and used primarily by the broader object permission checking infrastructure