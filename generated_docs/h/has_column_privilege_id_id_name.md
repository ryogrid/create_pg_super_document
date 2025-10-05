# has_column_privilege_id_id_name

## Location
[src/backend/utils/adt/acl.c:2738-2762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2738-L2762)

## Overview
This function checks column-level privileges for a specific role by taking a role OID, table OID, column name as text, and privilege type as text, returning whether the role has the specified privilege on the named column.

## Definition
```c
Datum has_column_privilege_id_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL built-in function provides column-level privilege checking using a combination of role OID (numeric identifier), table OID (numeric identifier), column name (as text), and privilege type (as text). This variant is efficient when you have both the role and table identifiers but need to specify the column by name.

The function performs column name resolution by converting the text column name to a column attribute number using convert_column_name with the provided table OID, converts the text privilege specification to an AclMode bitmask, and then delegates the privilege verification to column_privilege_check. Since both role and table are already provided as OIDs, this avoids the overhead of name resolution for those entities. If error conditions are encountered (such as a non-existent column), the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
- `roleid` (Oid): The object identifier of the role whose privileges are being checked
- `tableoid` (Oid): The object identifier of the table containing the column
- `column` (text): The name of the column whose privileges are being checked
- `priv_type_text` (text): The privilege type being checked (e.g., 'SELECT', 'UPDATE', 'INSERT', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_column_name](../c/convert_column_name.md): Converts text column name to attribute number
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts text privilege specification to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_OID: Extracts OID argument from function call
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking system for column-level access control
- Located in src/backend/utils/adt/acl.c at lines 2738-2762
- Returns NULL if the privilege check encounters error conditions (missing column, etc.)
- Part of a family of has_column_privilege functions with different parameter combinations
- More efficient than name-based variants since it avoids role and table name resolution
- Useful when you have numeric identifiers for role and table but need to specify the column by name
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS

## Simplified Source

```c
Datum has_column_privilege_id_id_name(PG_FUNCTION_ARGS) {
    // Extract arguments
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    text *column = PG_GETARG_TEXT_PP(2);
    text *priv_type_text = PG_GETARG_TEXT_PP(3);

    // Convert column name to attribute number
    AttrNumber colattnum = convert_column_name(tableoid, column);

    // Convert privilege string to ACL mode
    AclMode mode = convert_column_priv_string(priv_type_text);

    // Check privilege and return result
    int privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}
```