# pg_parameter_acl_aclmask

## Location
src/backend/catalog/aclchk.c: 3533 - 3591

## Overview
A function that examines a user's privileges for PostgreSQL configuration parameters by looking up the access control list using the parameter's pg_parameter_acl OID.

## Definition


## Detailed Description
This function provides privilege checking for PostgreSQL configuration parameters when the parameter is identified by its pg_parameter_acl catalog entry OID rather than by name. It serves as a complementary function to , offering a more direct lookup mechanism when the OID is already known.

The function workflow:
1. **Superuser Bypass**: Immediately grants all requested permissions to superusers
2. **Direct OID Lookup**: Uses the provided OID to directly access the pg_parameter_acl entry
3. **Error Handling**: Throws an error if the specified ACL entry doesn't exist
4. **ACL Processing**: Retrieves and processes the parameter's access control list
5. **Default Handling**: Applies default ACL permissions if none are explicitly defined
6. **Permission Evaluation**: Uses the standard aclmask function to determine final permissions

This approach is more efficient than name-based lookup when the OID is already available, avoiding the need for name conversion and text processing.

## Parameters / Member Variables
- : The OID of the pg_parameter_acl entry to check permissions for
- : The OID of the role whose permissions are being checked
- : Bitmask specifying which permissions to check (typically ACL_SET for parameter modification)
- : Specifies how to combine multiple ACL entries (ACLMASK_ALL or ACLMASK_ANY)

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - SearchSysCache1
  - SysCacheGetAttr
  - acldefault
  - DatumGetAclP
  - aclmask
  - ReleaseSysCache
  - pfree
- Called from (representative examples):
  - InternalDefaultACL
  - pg_aclmask

## Notes and Other Information
- This is a static (internal) function, not directly accessible outside aclchk.c
- Uses the PARAMETERACLOID system cache for efficient OID-based lookups
- Throws ERRCODE_UNDEFINED_OBJECT error if the specified ACL entry doesn't exist
- Part of PostgreSQL's parameter access control system that complements name-based lookup
- Uses BOOTSTRAP_SUPERUSERID as the owner when creating default ACL permissions
- More efficient than name-based lookup when the OID is already known
- Proper memory management includes cleanup of detoasted ACL data
- The function assumes the ACL entry should exist, unlike the name-based variant that handles missing entries gracefully