# has_tablespace_privilege_id

## Location
[src/backend/utils/adt/acl.c:4287-4314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4287-L4314)

## Overview
PostgreSQL built-in function that checks whether the current user has specific privileges on a tablespace identified by OID.

## Definition
```c
Datum has_tablespace_privilege_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `has_tablespace_privilege(tablespace_oid, privilege)` where the tablespace is specified by OID and the privilege as a text string. It assumes the current user as the subject of the privilege check. The function operates by:

1. Getting the current user's OID using `GetUserId()`
2. Converting the privilege string to an `AclMode` representation
3. Performing an extended ACL check that can detect missing tablespaces
4. Returning NULL if the tablespace doesn't exist, or boolean result otherwise

This variant combines the efficiency of OID-based tablespace identification with extended error handling capabilities, making it suitable for cases where the tablespace might not exist.

## Parameters / Member Variables
- `tablespaceoid` (Oid): Object identifier of the tablespace to check privileges for
- `priv_type_text` (text): Privilege type as a string (e.g., 'CREATE', 'ALL')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md) (to get current user's OID)
  - [convert_tablespace_priv_string](../c/convert_tablespace_priv_string.md) (converts privilege string to AclMode)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md) (performs extended privilege check with missing object detection)
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- This function assumes the current user as the subject of the privilege check
- Returns NULL if the tablespace object is missing rather than throwing an error
- Uses extended ACL checking for better error handling
- Part of the overloaded `has_tablespace_privilege` function family
- Located in `src/backend/utils/adt/acl.c:4287-4314`
- More efficient than name-based lookups since it directly uses the tablespace OID