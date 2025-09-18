# has_any_column_privilege_name

## Location
src/backend/utils/adt/acl.c: 2364 - 2391

## Overview
Checks if the current user has any given privilege on any column of a specified table (by name).

## Definition


## Detailed Description
This function is a variant of the column privilege checking functions that assumes the current user as the subject of the privilege check. It takes only two arguments: a table name and privilege type, automatically using the currently connected user's ID for the privilege check.

Like its counterpart , this function performs hierarchical privilege checking by first examining table-level privileges and then checking individual column privileges if needed. The function uses  to automatically determine the current user, making it convenient for checking the current user's own privileges without explicitly specifying the user.

This function is commonly used in SQL queries where users want to check their own privileges on table columns without needing to specify their username explicitly.

## Parameters / Member Variables
-  (tablename): The name of the table to check column privileges on
-  (priv_type_text): The privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE')

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the OID of the currently connected user
  - : Converts table name to its OID
  - : Converts privilege string to AclMode bitmask
  - : Checks table-level privileges
  - : Checks column-level privileges across all columns
  - : Constant used for checking if any column has the privilege
- Called from (representative examples):
  - SQL queries via function call interface (no direct C callers found)

## Notes and Other Information
- Returns a boolean value: true if the current user has the specified privilege on any column, false otherwise
- This is a convenience variant that automatically uses the current user, reducing the need to specify the username
- Part of the overloaded 'has_any_column_privilege' family of functions at the SQL level
- Performs the same two-tier privilege checking as other variants: table-level first, then column-level
- Located in src/backend/utils/adt/acl.c:2364-2391