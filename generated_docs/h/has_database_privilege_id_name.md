# has_database_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:3098-3120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3098-L3120)

## Overview
Checks database privileges for a specific user given the user's role ID, database name, and privilege name.

## Definition

```c
Datum
has_database_privilege_id_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This SQL-callable function determines whether a user (specified by role OID) has specific privileges on a database (specified by name). It's part of PostgreSQL's access control privilege checking system. The function converts the textual database name to an OID, converts the privilege string to an internal privilege bitmask, and then performs the actual privilege check using the standard object access control mechanism.

## Parameters / Member Variables
- : Role OID of the user whose privileges are being checked
- : Text name of the database to check privileges on  
- : Text string specifying the privilege type to check (e.g., "CREATE", "CONNECT", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_database_name](../c/convert_database_name.md): Converts database name to OID
  - [convert_database_priv_string](../c/convert_database_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual privilege check
  - AclResult: Enum type for access control results
- Called from (representative examples):
  - This function is typically called from SQL queries using the has_database_privilege() function

## Notes and Other Information
- Returns a boolean Datum: true if the user has the specified privilege, false otherwise
- Part of the has_database_privilege family of functions that provide different parameter combinations
- The function performs error checking through the convert_* helper functions
- Located in src/backend/utils/adt/acl.c:3098-3120