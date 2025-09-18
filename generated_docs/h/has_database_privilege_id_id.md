# has_database_privilege_id_id

## Location
src/backend/utils/adt/acl.c: 3121 - 3149

## Overview
Checks database privileges for a specific user given the user role ID, database OID, and privilege name.

## Definition
```c
Datum has_database_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function determines whether a user (specified by role OID) has specific privileges on a database (specified by database OID). Unlike the name variant, this function uses the extended access control check that can detect missing objects. If the database does not exist, the function returns NULL instead of raising an error, providing more graceful handling of non-existent databases.

## Parameters / Member Variables
- `PG_GETARG_OID(0)`: Role OID of the user whose privileges are being checked
- `PG_GETARG_OID(1)`: Database OID to check privileges on
- `PG_GETARG_TEXT_PP(2)`: Text string specifying the privilege type to check (e.g., "CREATE", "CONNECT", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_database_priv_string](../c/convert_database_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Extended privilege check that can detect missing objects
  - AclResult: Enum type for access control results
- Called from (representative examples):
  - This function is typically called from SQL queries using the has_database_privilege() function

## Notes and Other Information
- Returns a boolean Datum: true if the user has the specified privilege, false if not, NULL if database does not exist
- Uses object_aclcheck_ext instead of object_aclcheck for better error handling
- Part of the has_database_privilege family of functions that provide different parameter combinations
- The is_missing flag allows detection of non-existent databases
- Located in src/backend/utils/adt/acl.c:3121-3149