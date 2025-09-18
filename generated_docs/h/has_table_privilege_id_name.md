# has_table_privilege_id_name

## Location
src/backend/utils/adt/acl.c: 1999 - 2021

## Overview
Checks whether a specified user (by OID) has a given privilege on a table (by name), providing precise user identification combined with human-readable table specification.

## Definition
```c
Datum has_table_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function variant takes a user OID, table name, and privilege type to perform access control checking. It provides a hybrid approach where the user is specified by their precise OID (avoiding name resolution ambiguity) while the table is specified by its human-readable name. Unlike the OID-based table variants, this function does not handle missing tables gracefully - it will fail if the table name cannot be resolved.

The function process:
1. Uses the provided user OID directly (no conversion needed)
2. Converts the table name to an OID using convert_table_name()  
3. Converts the privilege string to an AclMode using convert_table_priv_string()
4. Performs the privilege check using pg_class_aclcheck()

## Parameters / Member Variables
- `roleid`: OID parameter specifying the exact user whose privileges are being checked
- `tablename`: Text parameter specifying the name of the table to check privileges on
- `priv_type_text`: Text parameter specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID - to extract OID argument
  - PG_GETARG_TEXT_PP - to extract text arguments
  - convert_table_name - converts table name to OID
  - convert_table_priv_string - converts privilege string to AclMode
  - pg_class_aclcheck - performs the actual ACL check
  - PG_RETURN_BOOL - returns boolean result
- Called from (representative examples):
  - SQL has_table_privilege() function calls with user OID and table name

## Notes and Other Information
- Returns true if the specified user has the privilege, false otherwise
- Uses precise OID for user identification, avoiding potential name resolution issues
- Does not handle missing tables gracefully - will error if table name is invalid
- All parameters are required and cannot be NULL
- Located in src/backend/utils/adt/acl.c:1999-2021
- Useful when caller has precise user OID but prefers human-readable table names