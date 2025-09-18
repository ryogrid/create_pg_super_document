# has_column_privilege_name_name_attnum

## Location
[src/backend/utils/adt/acl.c:2607-2633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2607-L2633)

## Overview
Checks user privileges on a specific column using the role name, table name, column attribute number, and privilege type as input parameters.

## Definition
```c
Datum has_column_privilege_name_name_attnum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides an interface for checking column privileges when the column is specified by its attribute number rather than name. It combines human-readable identifiers (role name, table name) with the internal column identifier (attribute number) to perform privilege verification. This variant is particularly useful when working with system catalog queries where attribute numbers are readily available.

Like other privilege checking functions, it converts the input parameters to their internal representations and delegates the actual privilege checking to the column_privilege_check helper function.

## Parameters / Member Variables
- `rolename` (Name): The name of the role whose privileges are being checked
- `tablename` (text*): Text string containing the name of the table
- `colattnum` (AttrNumber): The attribute number of the column (16-bit integer)
- `priv_type_text` (text*): Text string specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Converts role name to OID, handling "public" role
  - [convert_table_name](../c/convert_table_name.md): Converts table name string to table OID
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts privilege text to AclMode
  - [column_privilege_check](../c/column_privilege_check.md): Performs the actual privilege verification
  - PG_GETARG_NAME: PostgreSQL macro for extracting Name arguments
  - PG_GETARG_INT16: PostgreSQL macro for extracting 16-bit integer arguments
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL function interface)

## Notes and Other Information
- Uses attribute number directly instead of converting from column name
- Returns NULL if table or role doesn't exist, or if attribute number is invalid
- Part of the SQL-callable has_column_privilege function family
- More efficient than name-based variants when attribute number is known
- Located in src/backend/utils/adt/acl.c:2607-2633