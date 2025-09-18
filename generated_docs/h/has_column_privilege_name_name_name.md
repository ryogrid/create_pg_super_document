# has_column_privilege_name_name_name

## Location
src/backend/utils/adt/acl.c: 2578 - 2606

## Overview
Checks user privileges on a specific column using string-based identifiers for the role name, table name, column name, and privilege type.

## Definition
```c
Datum has_column_privilege_name_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a user-friendly interface for checking column privileges by accepting human-readable names rather than internal object identifiers. It serves as a wrapper that converts string identifiers to their corresponding OIDs and then delegates the actual privilege checking to the column_privilege_check helper function.

The function is designed to be called from SQL as part of PostgreSQL's privilege checking system, allowing users to query column-level permissions using familiar table and column names rather than internal system identifiers.

## Parameters / Member Variables
- `rolename` (Name): The name of the role whose privileges are being checked
- `tablename` (text*): Text string containing the name of the table
- `column` (text*): Text string containing the name of the column
- `priv_type_text` (text*): Text string specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Converts role name to OID, handling "public" role
  - convert_table_name: Converts table name string to table OID
  - convert_column_name: Converts column name string to attribute number
  - convert_column_priv_string: Converts privilege text to AclMode
  - column_privilege_check: Performs the actual privilege verification
  - PG_GETARG_NAME: PostgreSQL macro for extracting Name arguments
- Called from (representative examples):
  - No direct references found in codebase (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL if table, column, or role doesn't exist (handled by column_privilege_check returning -1)
- Part of the SQL-callable has_column_privilege function family
- All four parameters are required and must be valid string identifiers
- Uses PostgreSQL's function call interface (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/acl.c:2578-2606