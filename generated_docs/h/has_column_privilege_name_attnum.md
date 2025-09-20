# has_column_privilege_name_attnum

## Location
[src/backend/utils/adt/acl.c:2816-2842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2816-L2842)

## Overview
A PostgreSQL built-in function that checks whether the current user has a specific privilege on a column, identified by table name (text) and column attribute number (integer).

## Definition

```c
Datum
has_column_privilege_name_attnum(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a variant of the column privilege checking functions that takes a table name as text and a column attribute number (AttrNumber) as an integer, rather than column name as text. It checks if the current user has the specified privilege on the given column. The function converts the table name to an OID using  and the privilege string to an ACL mode using . Unlike , this function doesn't need to convert a column name since it already receives the attribute number directly. It performs the privilege check using  and returns NULL if there's an error (negative result) or a boolean indicating the privilege status.

## Parameters / Member Variables
-  (tablename): Text string representing the name of the table containing the column
-  (colattnum): Integer representing the attribute number of the column (1-based index)
-  (priv_type_text): Text string representing the privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - PG_GETARG_INT16
  - [convert_table_name](../c/convert_table_name.md)
  - [convert_column_priv_string](../c/convert_column_priv_string.md)
  - [column_privilege_check](../c/column_privilege_check.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function assumes the current user (obtained via GetUserId()) as the subject of the privilege check
- Takes the column attribute number directly as an integer parameter, avoiding the need for column name resolution
- Attribute numbers in PostgreSQL are 1-based (first column is 1, not 0)
- Returns NULL if the table doesn't exist or other error conditions occur
- Part of PostgreSQL's SQL-accessible privilege checking system, callable from SQL as has_column_privilege(table_name, column_attnum, privilege)
- Located in src/backend/utils/adt/acl.c:2816-2842