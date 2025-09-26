# has_table_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:1945-1972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1945-L1972)

## Overview
Checks whether a specified user has a given privilege on a table identified by OID, handling cases where the table may not exist by returning NULL.

## Definition
```c
Datum has_table_privilege_name_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function variant takes a user name, table OID, and privilege type to perform access control checking. A key feature is its graceful handling of non-existent tables - if the provided table OID does not correspond to an existing table, the function returns NULL rather than failing with an error. This behavior was introduced in PostgreSQL 8.4 to provide more robust error handling.

The function process:
1. Converts the user name to an OID using get_role_oid_or_public()
2. Converts the privilege string to an AclMode using convert_table_priv_string()
3. Performs privilege checking using pg_class_aclcheck_ext() which also indicates if the table is missing
4. Returns NULL if the table doesn't exist, otherwise returns the privilege check result

## Parameters / Member Variables
- `username`: Name type parameter specifying the user whose privileges are being checked
- `tableoid`: OID parameter identifying the specific table to check privileges on  
- `priv_type_text`: Text parameter specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME - to extract Name argument
  - PG_GETARG_OID - to extract OID argument
  - PG_GETARG_TEXT_PP - to extract text argument
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md) - converts role name to OID
  - [convert_table_priv_string](../c/convert_table_priv_string.md) - converts privilege string to AclMode
  - [pg_class_aclcheck_ext](../p/pg_class_aclcheck_ext.md) - performs ACL check with missing table detection
  - PG_RETURN_NULL - returns NULL for missing tables
  - PG_RETURN_BOOL - returns boolean result for existing tables
- Called from (representative examples):
  - SQL has_table_privilege() function calls with user name and table OID

## Notes and Other Information
- Returns true if user has the privilege, false if not, NULL if table doesn't exist
- Uses pg_class_aclcheck_ext() instead of pg_class_aclcheck() for missing table detection
- Introduced graceful NULL return behavior in PostgreSQL 8.4
- All parameters are required and cannot be NULL
- Located in src/backend/utils/adt/acl.c:1945-1972
- Useful when checking privileges on potentially non-existent table OIDs