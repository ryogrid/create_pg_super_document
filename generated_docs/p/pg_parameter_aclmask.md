# pg_parameter_aclmask

## Location
[src/backend/catalog/aclchk.c:3469-3532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3469-L3532)

## Overview
A function that examines a user's privileges for PostgreSQL configuration parameters (GUCs) by checking against the pg_parameter_acl system catalog.

## Definition

```c
static AclMode
pg_parameter_aclmask(const char *name, Oid roleid, AclMode mask, AclMaskHow how)
```
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
  - [superuser_arg](../s/superuser_arg.md)
  - [convert_GUC_name_for_parameter_acl](../c/convert_GUC_name_for_parameter_acl.md)
  - [cstring_to_text](../c/cstring_to_text.md)
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

## Simplified Source

```c
// Simplified version of pg_parameter_aclmask
static AclMode
pg_parameter_aclmask(const char *name, Oid roleid, AclMode mask, AclMaskHow how)
{
    AclMode result;
    char *parname;
    text *partext;
    HeapTuple tuple;

    // Superusers bypass all permission checking
    if (superuser_arg(roleid))
        return mask;

    // Convert GUC name to standardized form for pg_parameter_acl lookup
    parname = convert_GUC_name_for_parameter_acl(name);
    partext = cstring_to_text(parname);

    // Look up parameter ACL in system catalog
    tuple = SearchSysCache1(PARAMETERACLNAME, PointerGetDatum(partext));

    if (!HeapTupleIsValid(tuple))
    {
        // No entry found - non-superusers have no rights by default
        result = ACL_NO_RIGHTS;
    }
    else
    {
        // Extract ACL from catalog entry
        Datum aclDatum = SysCacheGetAttr(PARAMETERACLNAME, tuple,
                                       Anum_pg_parameter_acl_paracl, &isNull);
        Acl *acl;

        if (isNull)
        {
            // No explicit ACL - use default permissions
            acl = acldefault(OBJECT_PARAMETER_ACL, BOOTSTRAP_SUPERUSERID);
        }
        else
        {
            // Use stored ACL (handle detoasting if needed)
            acl = DatumGetAclP(aclDatum);
        }

        // Evaluate permissions against the ACL
        result = aclmask(acl, roleid, BOOTSTRAP_SUPERUSERID, mask, how);

        // Clean up resources
        if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
            pfree(acl);
        ReleaseSysCache(tuple);
    }

    // Clean up converted parameter name and text
    pfree(parname);
    pfree(partext);

    return result;
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added descriptive comments explaining each major step
- Simplified ACL extraction logic while preserving the null-check branching
- Maintained essential error handling and memory management
- Preserved the core algorithm: superuser check → name conversion → catalog lookup → ACL evaluation
- Combined cleanup operations with clearer organization