# object_aclcheck_ext

## Location
[src/backend/catalog/aclchk.c:3903-3924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3903-L3924)

## Overview
This is the extended version of object_aclcheck that provides access privilege checking for database objects with optional graceful handling of missing objects.

## Definition
AclResult object_aclcheck_ext(Oid classid, Oid objectid, Oid roleid, AclMode mode, bool *is_missing)

## Detailed Description
This function is the core implementation for PostgreSQL object access control checking with enhanced error handling capabilities. It delegates the actual permission checking to object_aclmask_ext and converts the returned mask into a simple ACLCHECK_OK/ACLCHECK_NO_PRIV result. The key enhancement over the basic object_aclcheck is the is_missing parameter, which allows the function to handle non-existent objects gracefully without throwing errors, making it suitable for conditional privilege checking scenarios.

## Parameters / Member Variables
- classid: The OID of the system catalog (pg_class, pg_proc, etc.) that contains the object being checked
- objectid: The OID of the specific object being checked for permissions  
- roleid: The OID of the role whose permissions are being verified
- mode: The access mode/permissions being requested (AclMode type)
- is_missing: Optional pointer to bool that gets set to true if the object does not exist (enables graceful error handling)

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclmask_ext](object_aclmask_ext.md)
  - ACLMASK_ANY (constant)
- Called from (representative examples):
  - [pg_namespace_aclmask_ext](../p/pg_namespace_aclmask_ext.md)
  - [object_aclcheck](object_aclcheck.md)
  - [has_database_privilege_name_id](../h/has_database_privilege_name_id.md) (and many other SQL privilege checking functions)
  - [has_function_privilege_id](../h/has_function_privilege_id.md)
  - [has_schema_privilege_id](../h/has_schema_privilege_id.md)

## Notes and Other Information
- Uses ACLMASK_ANY mode when calling object_aclmask_ext, meaning it checks if the user has ANY of the requested privileges
- Returns ACLCHECK_OK if any requested privileges are granted, ACLCHECK_NO_PRIV otherwise
- The is_missing parameter allows callers to distinguish between "access denied" and "object does not exist"
- Extensively used by SQL privilege checking functions like has_*_privilege_* family of functions
- Serves as the foundation for both simple privilege checks (when is_missing is NULL) and conditional checks (when is_missing is provided)

## Simplified Source

```c
// Simplified version of object_aclcheck_ext
AclResult
object_aclcheck_ext(Oid classid, Oid objectid, Oid roleid, AclMode mode, bool *is_missing)
{
    // Check if user has any of the requested privileges on the object
    if (object_aclmask_ext(classid, objectid, roleid, mode, ACLMASK_ANY, is_missing) != 0)
        return ACLCHECK_OK;        // User has required privileges
    else
        return ACLCHECK_NO_PRIV;   // User lacks required privileges
}
```

Key simplifications made:
- Added clear comments explaining the core logic flow
- Maintained the essential binary decision logic (has privileges vs. doesn't have privileges)
- Preserved the delegation to object_aclmask_ext which handles the complex privilege checking
- Kept the is_missing parameter handling intact as it's the key differentiator of this function