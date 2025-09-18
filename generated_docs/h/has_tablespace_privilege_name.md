# has_tablespace_privilege_name

## Location
src/backend/utils/adt/acl.c: 4233 - 4256

## Overview
PostgreSQL built-in function that checks whether the current user has specific privileges on a tablespace, identified by name.

## Definition
```c
Datum has_tablespace_privilege_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `has_tablespace_privilege(tablespace, privilege)` where both parameters are provided as text strings. It checks if the current user (determined by `GetUserId()`) has the specified privilege on the named tablespace. The function operates by:

1. Converting the tablespace name text to its corresponding OID
2. Converting the privilege string to an `AclMode` representation  
3. Performing an ACL (Access Control List) check using the object access control system
4. Returning a boolean result indicating whether the privilege check succeeded

This is one of several overloaded variants of the `has_tablespace_privilege` function family that differ in how they identify the user and tablespace (by name vs OID).

## Parameters / Member Variables
- `tablespacename` (text): Name of the tablespace to check privileges for
- `priv_type_text` (text): Privilege type as a string (e.g., 'CREATE', 'ALL')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md) (to get current user's OID)
  - [convert_tablespace_name](../c/convert_tablespace_name.md) (converts tablespace name to OID)
  - [convert_tablespace_priv_string](../c/convert_tablespace_priv_string.md) (converts privilege string to AclMode)
  - [object_aclcheck](../o/object_aclcheck.md) (performs the actual privilege check)
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- This function assumes the current user as the subject of the privilege check
- Part of PostgreSQL's SQL-accessible privilege checking system
- Returns boolean true if privilege exists, false otherwise
- Located in `src/backend/utils/adt/acl.c:4233-4256`