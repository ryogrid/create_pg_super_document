# has_table_privilege_id_id

## Location
src/backend/utils/adt/acl.c: 2022 - 2048

## Overview
Checks user privileges on a table given a role ID, table OID, and privilege name text string.

## Definition
```c
Datum has_table_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL system function that verifies whether a specific user (identified by role OID) has certain privileges on a table (identified by table OID). It accepts three parameters through the PostgreSQL function argument mechanism (PG_FUNCTION_ARGS) and returns a boolean datum indicating whether the user has the requested privileges. The function handles missing objects by returning NULL when the table doesn't exist.

The function follows the standard PostgreSQL privilege checking workflow:
1. Extracts the role OID, table OID, and privilege string from function arguments
2. Converts the privilege string to an internal privilege mode representation
3. Performs the actual privilege check using PostgreSQL's access control system
4. Returns appropriate result based on the check outcome

## Parameters / Member Variables
-  (Oid): The OID of the role/user whose privileges are being checked
-  (Oid): The OID of the table on which privileges are being checked  
-  (text*): Text string specifying the privilege type(s) to check (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - convert_table_priv_string: Converts privilege string to AclMode bitmask
  - pg_class_aclcheck_ext: Performs the actual privilege check on the table
  - AclResult: Enumeration type for access control check results
- Called from (representative examples):
  - This is a system function callable from SQL queries via has_table_privilege() function

## Notes and Other Information
- This function is part of PostgreSQL's privilege inquiry system functions
- Returns NULL when the specified table does not exist (is_missing flag)
- Uses PostgreSQL's internal access control list (ACL) system for privilege verification
- The function is typically invoked through SQL function calls rather than direct C code calls
- Located in src/backend/utils/adt/acl.c:2022-2048