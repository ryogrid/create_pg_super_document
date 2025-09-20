# has_column_privilege_id_attnum

## Location
[src/backend/utils/adt/acl.c:2870-2897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2870-L2897)

## Overview
A PostgreSQL built-in function that checks whether the current user has a specific privilege on a column, identified by table OID and column attribute number (both as direct identifiers).

## Definition

```c
Datum
has_column_privilege_id_attnum(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the most efficient variant of the column privilege checking functions, as it takes both the table OID and column attribute number directly as parameters, eliminating the need for any name-to-identifier conversions. It checks if the current user has the specified privilege on the column identified by the table OID and attribute number. Since both the table and column are already identified by their internal identifiers, the function only needs to convert the privilege string to an ACL mode using  before performing the privilege check with . This makes it the fastest of the four has_column_privilege variants.

## Parameters / Member Variables
-  (tableoid): Object identifier (OID) of the table containing the column
-  (colattnum): Integer representing the attribute number of the column (1-based index)
-  (priv_type_text): Text string representing the privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - PG_GETARG_OID
  - PG_GETARG_INT16
  - [convert_column_priv_string](../c/convert_column_priv_string.md)
  - [column_privilege_check](../c/column_privilege_check.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function assumes the current user (obtained via GetUserId()) as the subject of the privilege check
- Most efficient variant of the has_column_privilege functions, requiring no name resolution
- Both table and column are identified by their internal PostgreSQL identifiers (OID and attribute number)
- Attribute numbers in PostgreSQL are 1-based (first column is 1, not 0)
- Returns NULL if error conditions occur (e.g., invalid OID or attribute number)
- Part of PostgreSQL's SQL-accessible privilege checking system, callable from SQL as has_column_privilege(table_oid, column_attnum, privilege)
- Located in src/backend/utils/adt/acl.c:2870-2897