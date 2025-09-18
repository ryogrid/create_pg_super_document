# has_function_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:3446-3475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3446-L3475)

## Overview
Checks whether a user has specific privileges on a function, using role name, function OID, and privilege type string as parameters.

## Definition
Datum has_function_privilege_name_id(PG_FUNCTION_ARGS)

## Detailed Description
This function is another variant in the PostgreSQL "has_function_privilege" family, designed to check user privileges when the function is identified by its OID rather than name. This variant is particularly useful when the function OID is already known, avoiding the overhead of name-to-OID conversion and potential ambiguity issues with overloaded function names.

The function implements a more robust privilege checking process:
1. Converts the username to a role OID, supporting both regular role names and the special "public" role
2. Converts the privilege string to an AclMode bitmask
3. Uses object_aclcheck_ext (the extended version) to perform the privilege check with explicit missing object detection
4. Returns NULL if the function object doesn't exist, true if the user has the privilege, false otherwise

The key difference from other variants is the use of object_aclcheck_ext, which provides explicit notification when the target object is missing, allowing for proper NULL return handling.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro, extracting:
  -  (Name): The name of the role to check privileges for
  -  (Oid): The object identifier of the function to check privileges on
  -  (text): The privilege type string (e.g., "EXECUTE")

## Dependencies
- Functions called/Symbols referenced:
  - Name: PostgreSQL name type
  - PG_GETARG_NAME: Macro to extract name argument from function call
  - PG_GETARG_OID: Macro to extract OID argument from function call
  - AclResult: Enumeration for access control check results
  - get_role_oid_or_public: Converts role name to OID, handling "public"
  - [convert_function_priv_string](../c/convert_function_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Extended access control check with missing object detection
- Called from (representative examples):
  - No direct references found (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as has_function_privilege(username, function_oid, privilege)
- Part of the has_function_privilege family providing different parameter combinations
- Uses object_aclcheck_ext instead of object_aclcheck for better missing object handling
- Returns BOOL type to SQL layer, or NULL if the function doesn't exist
- More efficient than name-based variants when the function OID is already available
- Eliminates function name resolution overhead and ambiguity with overloaded functions
- The is_missing flag provides explicit detection of non-existent functions
- Handles the "public" role specially through get_role_oid_or_public
- Uses ProcedureRelationId to indicate that privilege checking is being done on functions/procedures