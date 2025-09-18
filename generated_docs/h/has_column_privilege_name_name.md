# has_column_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 2787 - 2815

## Overview
A PostgreSQL built-in function that checks whether the current user has a specific privilege on a column, identified by table name and column name (both as text strings).

## Definition


## Detailed Description
This function is one of PostgreSQL's privilege checking functions that determines if the current user has specific access rights on a column. It takes three text parameters: table name, column name, and privilege type. The function internally converts the text table name to an OID using , the column name to an attribute number using , and the privilege string to an ACL mode. It then performs the actual privilege check using the  helper function. If the privilege check returns a negative value (indicating an error condition like a non-existent table or column), the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
-  (tablename): Text string representing the name of the table containing the column
-  (column): Text string representing the name of the column to check privileges for
-  (priv_type_text): Text string representing the privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [convert_table_name](../c/convert_table_name.md)
  - [convert_column_name](../c/convert_column_name.md)
  - [convert_column_priv_string](../c/convert_column_priv_string.md)
  - [column_privilege_check](../c/column_privilege_check.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function assumes the current user (obtained via GetUserId()) as the subject of the privilege check
- Returns NULL if the table or column doesn't exist, following PostgreSQL's convention for privilege functions
- Part of PostgreSQL's SQL-accessible privilege checking system, callable from SQL as has_column_privilege(table_name, column_name, privilege)
- Located in src/backend/utils/adt/acl.c:2787-2815