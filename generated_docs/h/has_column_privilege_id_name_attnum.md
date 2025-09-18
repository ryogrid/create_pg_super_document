# has_column_privilege_id_name_attnum

## Location
src/backend/utils/adt/acl.c: 2713 - 2737

## Overview
This function checks column-level privileges for a specific role by taking a role OID, table name as text, column attribute number as integer, and privilege type as text, returning whether the role has the specified privilege on the column.

## Definition
```c
Datum has_column_privilege_id_name_attnum(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL built-in function provides column-level privilege checking using a combination of role OID (numeric identifier), table name (as text), column attribute number (as integer), and privilege type (as text). This variant is useful when you have the role ID and know the specific column number but need to specify the table by name.

The function performs table name resolution by converting the text table name to a table OID using convert_table_name, converts the text privilege specification to an AclMode bitmask, and then delegates the privilege verification to column_privilege_check. The column is identified directly by its attribute number, avoiding the need for column name resolution. If error conditions are encountered (such as a non-existent table or invalid column number), the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
- `roleid` (Oid): The object identifier of the role whose privileges are being checked
- `tablename` (text): The name of the table containing the column (as text string)
- `colattnum` (AttrNumber/int16): The attribute number (column number) within the table
- `priv_type_text` (text): The privilege type being checked (e.g., 'SELECT', 'UPDATE', 'INSERT', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_table_name](../c/convert_table_name.md): Converts text table name to table OID
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts text privilege specification to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_OID: Extracts OID argument from function call
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
  - PG_GETARG_INT16: Extracts int16 argument from function call
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking system for column-level access control
- Located in src/backend/utils/adt/acl.c at lines 2713-2737
- Returns NULL if the privilege check encounters error conditions (missing table, invalid column number, etc.)
- Part of a family of has_column_privilege functions with different parameter combinations
- Useful when you have a role OID and column number but need to specify the table by name
- More efficient than the name-based column variant since it avoids column name resolution
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS