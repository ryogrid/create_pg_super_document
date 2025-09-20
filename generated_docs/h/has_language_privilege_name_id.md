# has_language_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:3655-3684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3655-L3684)

## Overview
This function checks whether a specified user has a particular privilege on a language, given the username and language OID as direct parameters.

## Definition

```c
Datum
has_language_privilege_name_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides an optimized variant of privilege checking where the language is specified by its OID rather than name, avoiding the need for name-to-OID conversion. It takes a username (Name type), language OID, and privilege type (text), then returns a boolean indicating whether the user has the requested privilege. This variant uses object_aclcheck_ext which provides explicit missing object detection, returning NULL when the language OID doesn't exist.

## Parameters / Member Variables
-  (Name): The name of the user/role whose privileges are being checked
-  (Oid): The OID of the procedural language to check privileges for
-  (text): The privilege type being checked (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extract Name argument from function call
  - PG_GETARG_OID: Extract OID argument from function call
  - PG_GETARG_TEXT_PP: Extract text argument from function call
  - get_role_oid_or_public: Convert username to role OID
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Convert privilege string to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Perform privilege check with missing object detection
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Uses object_aclcheck_ext instead of object_aclcheck for explicit missing object detection
- More efficient than name-based variants as it avoids language name lookup
- Explicitly returns NULL if the language OID doesn't exist (rather than raising an error)
- Part of PostgreSQL's access control system for procedural languages
- All has_language_privilege variants are named 'has_language_privilege' at the SQL level
- Located in src/backend/utils/adt/acl.c:3655-3684