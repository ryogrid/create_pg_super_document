# has_table_privilege_id

## Location
src/backend/utils/adt/acl.c: 1973 - 1998

## Overview
Checks whether the current user has a given privilege on a table identified by OID, returning NULL if the table does not exist.

## Definition
```c
Datum has_table_privilege_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function combines the convenience of assuming the current user context with the robustness of OID-based table identification and graceful handling of missing tables. It checks whether the currently executing user has a specific privilege on a table identified by its OID. Like other OID-based variants, it returns NULL if the specified table OID does not exist, rather than throwing an error.

The function workflow:
1. Gets the current user ID using GetUserId()
2. Converts the privilege string to an AclMode using convert_table_priv_string()
3. Performs privilege checking using pg_class_aclcheck_ext() with missing table detection
4. Returns NULL if table is missing, otherwise returns the boolean privilege check result

## Parameters / Member Variables
- `tableoid`: OID parameter identifying the specific table to check privileges on
- `priv_type_text`: Text parameter specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID - to extract OID argument
  - PG_GETARG_TEXT_PP - to extract text argument
  - [GetUserId](../G/GetUserId.md) - retrieves current session user ID
  - [convert_table_priv_string](../c/convert_table_priv_string.md) - converts privilege string to AclMode  
  - [pg_class_aclcheck_ext](../p/pg_class_aclcheck_ext.md) - performs ACL check with missing table detection
  - PG_RETURN_NULL - returns NULL for missing tables
  - PG_RETURN_BOOL - returns boolean result for existing tables
- Called from (representative examples):
  - SQL has_table_privilege() function calls with table OID only

## Notes and Other Information
- Returns true if current user has the privilege, false if not, NULL if table doesn't exist
- Automatically uses current session user, eliminating need to specify user explicitly
- Uses pg_class_aclcheck_ext() for robust missing table handling
- Both parameters are required and cannot be NULL
- Located in src/backend/utils/adt/acl.c:1973-1998
- Ideal for privilege checks on potentially non-existent tables in current user context