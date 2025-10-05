# has_column_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:2843-2869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2843-L2869)

## Overview
A PostgreSQL built-in function that checks whether the current user has a specific privilege on a column, identified by table OID and column name (text string).

## Definition

```c
Datum
has_column_privilege_id_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is another variant of the column privilege checking functions that takes a table OID directly as the first parameter instead of a table name. It checks if the current user has the specified privilege on a column identified by the table OID and column name. Since it already has the table OID, it skips the table name resolution step () and directly converts the column name to an attribute number using . The privilege string is converted to an ACL mode using , and the actual privilege check is performed using . This variant is more efficient when the table OID is already known, avoiding the overhead of name-to-OID resolution.

## Parameters / Member Variables
-  (tableoid): Object identifier (OID) of the table containing the column
-  (column): Text string representing the name of the column to check privileges for
-  (priv_type_text): Text string representing the privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - PG_GETARG_OID
  - [convert_column_name](../c/convert_column_name.md)
  - [convert_column_priv_string](../c/convert_column_priv_string.md)
  - [column_privilege_check](../c/column_privilege_check.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function assumes the current user (obtained via GetUserId()) as the subject of the privilege check
- More efficient than name-based variants when the table OID is already available, as it avoids table name resolution
- The table OID parameter allows for direct access without namespace resolution overhead
- Returns NULL if the column doesn't exist in the specified table or other error conditions occur
- Part of PostgreSQL's SQL-accessible privilege checking system, callable from SQL as has_column_privilege(table_oid, column_name, privilege)
- Located in src/backend/utils/adt/acl.c:2843-2869

## Simplified Source

```c
Datum has_column_privilege_id_name(PG_FUNCTION_ARGS) {
    // Extract arguments
    Oid tableoid = PG_GETARG_OID(0);
    text *column = PG_GETARG_TEXT_PP(1);
    text *priv_type_text = PG_GETARG_TEXT_PP(2);

    // Get current user and convert column name
    Oid roleid = GetUserId();
    AttrNumber colattnum = convert_column_name(tableoid, column);
    AclMode mode = convert_column_priv_string(priv_type_text);

    // Check privilege and return result
    int privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}
```