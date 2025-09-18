# pg_parameter_aclmask

## Location
[src/backend/catalog/aclchk.c:3469-3532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3469-L3532)

## Overview
A function that examines a user's privileges for PostgreSQL configuration parameters (GUCs) by checking against the pg_parameter_acl system catalog.

## Definition


## Detailed Description
This function implements privilege checking for PostgreSQL configuration parameters (GUCs - Grand Unified Configuration). It provides fine-grained access control over who can modify specific configuration settings. The function:

1. **Superuser Bypass**: Allows superusers to bypass all permission checks for any parameter
2. **Parameter Name Conversion**: Converts the GUC name to the standardized form used in pg_parameter_acl
3. **ACL Lookup**: Searches the pg_parameter_acl system catalog for the parameter's access control list
4. **Permission Evaluation**: If no explicit ACL exists, defaults to no rights for non-superusers; otherwise evaluates the stored ACL
5. **Memory Management**: Properly handles memory allocation and cleanup for text conversion and ACL processing

This is part of PostgreSQL's security framework that allows administrators to control which users can modify specific configuration parameters beyond the traditional superuser-only model.

## Parameters / Member Variables
- : The name of the configuration parameter (GUC) to check permissions for
- : The OID of the role whose permissions are being checked
- : Bitmask specifying which permissions to check (typically ACL_SET for parameter modification)
- : Specifies how to combine multiple ACL entries (ACLMASK_ALL or ACLMASK_ANY)

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - [convert_GUC_name_for_parameter_acl](../c/convert_GUC_name_for_parameter_acl.md)
  - cstring_to_text
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [pfree](pfree.md)
- Called from (representative examples):
  - [pg_parameter_aclcheck](pg_parameter_aclcheck.md)

## Notes and Other Information
- This is a static (internal) function, not directly accessible outside aclchk.c
- Uses the PARAMETERACLNAME system cache for efficient lookups
- Non-superusers have no rights by default if no explicit ACL entry exists for the parameter
- The function handles GUC name normalization to ensure consistent lookup in pg_parameter_acl
- Part of PostgreSQL's enhanced security model introduced for fine-grained parameter access control
- Default ACL uses BOOTSTRAP_SUPERUSERID as the owner when creating default permissions
- Proper memory management includes cleanup of both the converted parameter name and text objects