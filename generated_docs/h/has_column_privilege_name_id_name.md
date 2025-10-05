# has_column_privilege_name_id_name

## Location
[src/backend/utils/adt/acl.c:2634-2660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2634-L2660)

## Overview
Checks user privileges on a specific column using a combination of role name, table OID, column name, and privilege type as input parameters.

## Definition
```c
Datum has_column_privilege_name_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a mixed-identifier interface for checking column privileges, combining a human-readable role name with an internal table OID and a column name string. This variant is useful when the table OID is readily available (such as from system catalog queries) but the role and column are specified by name.

The function follows the standard pattern of converting input parameters to their internal representations and delegating the actual privilege checking to the column_privilege_check helper function.

## Parameters / Member Variables
- `username` (Name): The name of the role whose privileges are being checked
- `tableoid` (Oid): The object identifier of the table containing the column
- `column` (text*): Text string containing the name of the column
- `priv_type_text` (text*): Text string specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md): Converts role name to OID, handling "public" role
  - [convert_column_name](../c/convert_column_name.md): Converts column name string to attribute number
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts privilege text to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_NAME: PostgreSQL macro for extracting Name arguments
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL function interface)

## Notes and Other Information
- Uses table OID directly, avoiding table name resolution overhead
- Returns NULL if role doesn't exist, column doesn't exist, or table OID is invalid
- Part of the SQL-callable has_column_privilege function family
- More efficient than fully name-based variants when table OID is known
- Located in src/backend/utils/adt/acl.c:2634-2660

## Simplified Source

```c
Datum
has_column_privilege_name_id_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    text *column = PG_GETARG_TEXT_PP(2);
    text *priv_type_text = PG_GETARG_TEXT_PP(3);
    Oid roleid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    // Convert identifiers to internal representations
    roleid = get_role_oid_or_public(NameStr(*username));
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    // Perform privilege check
    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}
```