# has_column_privilege_id_name_name

## Location
[src/backend/utils/adt/acl.c:2686-2712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2686-L2712)

## Overview
This function checks column-level privileges for a specific role by taking a role OID, table name as text, column name as text, and privilege type as text, returning whether the role has the specified privilege on the named column.

## Definition
```c
Datum has_column_privilege_id_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL built-in function provides column-level privilege checking using a combination of role OID (numeric identifier), table name (as text), column name (as text), and privilege type (as text). It offers a convenient interface for privilege verification when you have the role ID but need to specify the table and column by name rather than numeric identifiers.

The function performs several conversion steps: it converts the text table name to a table OID using convert_table_name, converts the text column name to a column attribute number using convert_column_name, and converts the text privilege specification to an AclMode bitmask. After these conversions, it delegates the actual privilege checking to column_privilege_check. The function returns NULL if error conditions are encountered (such as non-existent table or column), otherwise returns a boolean indicating privilege status.

## Parameters / Member Variables
- `roleid` (Oid): The object identifier of the role whose privileges are being checked
- `tablename` (text): The name of the table containing the column (as text string)
- `column` (text): The name of the column whose privileges are being checked
- `priv_type_text` (text): The privilege type being checked (e.g., 'SELECT', 'UPDATE', 'INSERT', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_table_name](../c/convert_table_name.md): Converts text table name to table OID
  - [convert_column_name](../c/convert_column_name.md): Converts text column name to attribute number
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts text privilege specification to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_OID: Extracts OID argument from function call
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking system for column-level access control
- Located in src/backend/utils/adt/acl.c at lines 2686-2712
- Returns NULL if the privilege check encounters error conditions (missing table, missing column, etc.)
- Part of a family of has_column_privilege functions with different parameter combinations
- Useful when you have a role OID but need to specify table and column names textually
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS