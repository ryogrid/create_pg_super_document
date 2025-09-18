# has_table_privilege_name

## Location
src/backend/utils/adt/acl.c: 1921 - 1944

## Overview
Checks whether the current user has a given privilege on a table, taking only the table name and privilege type as parameters.

## Definition
```c
Datum has_table_privilege_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a variant of the has_table_privilege family that assumes the current user context. It checks whether the currently executing user has a specific privilege on a specified table. This is a convenience function that eliminates the need to specify the user explicitly, automatically using the current session's user ID.

The function workflow:
1. Gets the current user ID using GetUserId()
2. Converts the table name to an OID using convert_table_name()
3. Converts the privilege string to an AclMode using convert_table_priv_string()  
4. Performs the privilege check using pg_class_aclcheck()

## Parameters / Member Variables
- `tablename`: Text parameter specifying the name of the table to check privileges on
- `priv_type_text`: Text parameter specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP - to extract text arguments
  - [GetUserId](../G/GetUserId.md) - retrieves current session user ID
  - [convert_table_name](../c/convert_table_name.md) - converts table name to OID
  - [convert_table_priv_string](../c/convert_table_priv_string.md) - converts privilege string to AclMode
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) - performs the actual ACL check
  - PG_RETURN_BOOL - returns boolean result
- Called from (representative examples):
  - SQL has_table_privilege() function calls with 2 parameters

## Notes and Other Information
- Returns true if current user has the specified privilege, false otherwise
- Automatically uses the current session user, no explicit user specification needed
- Both parameters are required and cannot be NULL
- This is one of several overloaded variants of has_table_privilege at the SQL level
- Located in src/backend/utils/adt/acl.c:1921-1944
- Commonly used in applications where privilege checks are needed for the current user