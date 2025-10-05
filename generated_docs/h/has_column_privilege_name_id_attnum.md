# has_column_privilege_name_id_attnum

## Location
[src/backend/utils/adt/acl.c:2661-2685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2661-L2685)

## Overview
This function checks column-level privileges for a specific user by taking a username (as a Name type), table OID, column attribute number, and privilege type as text, returning whether the user has the specified privilege on the column.

## Definition
```c
Datum has_column_privilege_name_id_attnum(PG_FUNCTION_ARGS)
```

## Detailed Description
This is one of the PostgreSQL built-in functions that provides column-level privilege checking functionality. It accepts a combination of user name (in Name format), table identifier (as OID), column attribute number (as int16), and privilege type (as text), then determines whether the specified user has the requested privilege on that specific column.

The function performs the privilege check by first resolving the username to a role OID, converting the text privilege specification to an AclMode bitmask, and then delegating the actual privilege verification to the column_privilege_check helper function. If the privilege check encounters an error condition (such as a dropped column or missing table), the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
- `username` (Name): The name of the user/role whose privileges are being checked
- `tableoid` (Oid): The object identifier of the table containing the column
- `colattnum` (AttrNumber/int16): The attribute number (column number) within the table
- `priv_type_text` (text): The privilege type being checked (e.g., 'SELECT', 'UPDATE', 'INSERT', 'REFERENCES')

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md): Converts username to role OID
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts text privilege specification to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_NAME: Extracts Name argument from function call
  - PG_GETARG_OID: Extracts OID argument from function call
  - PG_GETARG_INT16: Extracts int16 argument from function call
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking system for column-level access control
- Located in src/backend/utils/adt/acl.c at lines 2661-2685
- Returns NULL if the privilege check encounters error conditions (missing table, dropped column, etc.)
- Part of a family of has_column_privilege functions with different parameter combinations
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS

## Simplified Source

```c
Datum
has_column_privilege_name_id_attnum(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text *priv_type_text = PG_GETARG_TEXT_PP(3);
    Oid roleid;
    AclMode mode;
    int privresult;

    // Convert identifiers to internal representations
    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_column_priv_string(priv_type_text);

    // Perform privilege check
    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}
```