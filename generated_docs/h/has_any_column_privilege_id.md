# has_any_column_privilege_id

## Location
[src/backend/utils/adt/acl.c:2427-2459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2427-L2459)

## Overview
Checks if the current user has any given privilege on any column of a specified table (by OID), with NULL handling for non-existent objects.

## Definition


## Detailed Description
This function combines the convenience of automatic current user detection with the precision of OID-based table identification and robust error handling. It automatically uses the currently connected user (via ) and takes a table OID for precise table identification.

Like , this function uses the extended ('_ext') variants of privilege checking functions to detect missing database objects. When the table OID doesn't correspond to an existing table or other objects are missing, it returns NULL instead of throwing an error, making it safe for use in SQL queries that might reference non-existent tables.

The function follows the standard two-tier privilege checking approach: first checking table-level privileges, then examining individual columns if the table-level check doesn't grant the privilege.

## Parameters / Member Variables
-  (tableoid): The OID of the table to check column privileges on
-  (priv_type_text): The privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE')

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the OID of the currently connected user
  - : Converts privilege string to AclMode bitmask
  - : Checks table-level privileges with missing object detection
  - : Checks column-level privileges with missing object detection
  - : Constant used for checking if any column has the privilege
- Called from (representative examples):
  - SQL queries via function call interface (no direct C callers found)

## Notes and Other Information
- Returns boolean for valid objects, NULL if the table OID doesn't exist or objects are missing
- Automatically uses the current user, eliminating the need to specify username
- The OID-based table identification provides better performance than name-based lookups
- Uses extended privilege checking functions for safe handling of missing objects
- Part of the overloaded 'has_any_column_privilege' family of functions at the SQL level
- Most efficient variant when you know the table OID and want to check current user's privileges
- Located in src/backend/utils/adt/acl.c:2427-2459