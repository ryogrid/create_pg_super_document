# has_column_privilege_id_id_attnum

## Location
[src/backend/utils/adt/acl.c:2763-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2763-L2786)

## Overview
This function checks column-level privileges for a specific role by taking a role OID, table OID, column attribute number as integer, and privilege type as text, providing the most efficient privilege check using all numeric identifiers.

## Definition
```c
Datum has_column_privilege_id_id_attnum(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL built-in function provides the most efficient column-level privilege checking variant, using numeric identifiers for role (OID), table (OID), and column (attribute number), with only the privilege type specified as text. This function avoids all name resolution overhead since all primary entities are identified numerically.

The function is streamlined compared to other variants - it only needs to convert the text privilege specification to an AclMode bitmask using convert_column_priv_string, then directly calls column_privilege_check with the provided numeric identifiers. This makes it the fastest of the has_column_privilege family functions since it performs minimal conversions. If error conditions are encountered (such as invalid identifiers), the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
- `roleid` (Oid): The object identifier of the role whose privileges are being checked
- `tableoid` (Oid): The object identifier of the table containing the column
- `colattnum` (AttrNumber/int16): The attribute number (column number) within the table
- `priv_type_text` (text): The privilege type being checked (e.g., 'SELECT', 'UPDATE', 'INSERT', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts text privilege specification to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_OID: Extracts OID argument from function call
  - PG_GETARG_INT16: Extracts int16 argument from function call
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking system for column-level access control
- Located in src/backend/utils/adt/acl.c at lines 2763-2786
- Returns NULL if the privilege check encounters error conditions (invalid identifiers, etc.)
- Part of a family of has_column_privilege functions with different parameter combinations
- Most efficient variant since it uses all numeric identifiers and avoids name resolution
- Ideal for internal PostgreSQL code or applications that already have numeric identifiers
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS

## Simplified Source

```c
Datum has_column_privilege_id_id_attnum(PG_FUNCTION_ARGS) {
    // Extract arguments
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text *priv_type_text = PG_GETARG_TEXT_PP(3);

    // Convert privilege string to ACL mode
    AclMode mode = convert_column_priv_string(priv_type_text);

    // Check privilege and return result
    int privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}
```