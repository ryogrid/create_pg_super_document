# has_any_column_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:2460-2486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2460-L2486)

## Overview
Checks if a specified user (by OID) has any given privilege on any column of a specified table (by name).

## Definition


## Detailed Description
This function represents the inverse approach of , taking a user OID and table name as parameters. This combination is useful when you have precise user identification (perhaps from a previous lookup or system context) but need to work with user-friendly table names.

Unlike the variants that use OID-based table identification, this function uses the standard (non-'_ext') privilege checking functions ( and ), which means it will throw errors rather than return NULL if the table name doesn't correspond to an existing table.

The function performs the same hierarchical privilege checking pattern: table-level privileges are checked first, and if that doesn't grant access, it then examines each column individually to see if the user has the specified privilege on any column.

## Parameters / Member Variables
-  (roleid): The OID of the role/user to check privileges for
-  (tablename): The name of the table to check column privileges on
-  (priv_type_text): The privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts table name to its OID
  - : Converts privilege string to AclMode bitmask
  - : Checks table-level privileges
  - : Checks column-level privileges across all columns
  - : Constant used for checking if any column has the privilege
- Called from (representative examples):
  - SQL queries via function call interface (no direct C callers found)

## Notes and Other Information
- Returns boolean value: true if the user has the specified privilege on any column, false otherwise
- Uses standard privilege checking functions that will raise errors for non-existent tables
- The user OID parameter allows for precise user identification when OIDs are already available
- Part of the overloaded 'has_any_column_privilege' family of functions at the SQL level
- Completes the set of variants covering all combinations of user/table identification methods
- Located in src/backend/utils/adt/acl.c:2460-2486