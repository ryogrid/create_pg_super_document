# has_any_column_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 2334 - 2363

## Overview
Checks if a specified user (by name) has any given privilege on any column of a specified table (by name).

## Definition


## Detailed Description
This function is part of PostgreSQL's access control system and serves as a SQL-callable function to determine whether a user has a specified privilege on any column of a table. It takes three text arguments: a role name, table name, and privilege type, then performs hierarchical privilege checking.

The function first performs a table-level privilege check using . If the user doesn't have the privilege at the table level, it then examines each individual column using  to see if the user has the privilege on any specific column. This approach is efficient as table-level privileges typically grant access to all columns.

The function is one of several variants of  that handle different combinations of user identification (name vs OID) and table identification (name vs OID).

## Parameters / Member Variables
-  (rolename): The name of the role/user to check privileges for
-  (tablename): The name of the table to check column privileges on
-  (priv_type_text): The privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts role name to OID, handling 'public' role specially
  - : Converts table name to its OID
  - : Converts privilege string to AclMode bitmask
  - : Checks table-level privileges
  - : Checks column-level privileges across all columns
  - : Constant used for checking if any column has the privilege
- Called from (representative examples):
  - SQL queries via function call interface (no direct C callers found)

## Notes and Other Information
- Returns a boolean value: true if the user has the specified privilege on any column, false otherwise
- This is one of multiple overloaded functions all named 'has_any_column_privilege' at the SQL level
- The function performs efficient two-tier checking: table-level first, then column-level if needed
- Part of PostgreSQL's comprehensive access control and privilege management system
- Located in src/backend/utils/adt/acl.c:2334-2363