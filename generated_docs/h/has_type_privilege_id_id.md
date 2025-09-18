# has_type_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:4537-4565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4537-L4565)

## Overview
Checks user privileges on a type given a role ID, type OID, and privilege name (as text).

## Definition
```c
Datum has_type_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that checks whether a specific user (identified by role ID) has specific privileges on a type (identified by OID). It takes a role OID, a type OID, and a privilege string as arguments. The function converts the privilege string to the appropriate access control mode and performs an access control check using the extended object access control framework. This variant provides the most direct access as both the role and type are specified by their OIDs, avoiding name resolution overhead.

## Parameters / Member Variables
- `roleid` (PG_GETARG_OID(0)): The object identifier of the role (user) to check privileges for
- `typeoid` (PG_GETARG_OID(1)): The object identifier of the type to check privileges for
- `priv_type_text` (PG_GETARG_TEXT_PP(2)): Text representation of the privilege(s) to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_type_priv_string](../c/convert_type_priv_string.md): Converts privilege text to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Performs the actual access control check with missing object detection
  - PG_RETURN_NULL: Returns NULL if type is missing
  - PG_RETURN_BOOL: Returns boolean result of privilege check
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function allows checking privileges for any role, not just the current user
- Uses object_aclcheck_ext which can detect missing objects and return NULL appropriately
- Most efficient variant as it uses OIDs directly without name resolution
- Returns NULL if the specified type does not exist (is_missing flag)
- Part of PostgreSQL's access control system for type privileges
- Typically used in SQL queries where both role OID and type OID are known
- Located in src/backend/utils/adt/acl.c:4537-4565