# has_language_privilege_id

## Location
[src/backend/utils/adt/acl.c:3685-3712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3685-L3712)

## Overview
This function checks whether the current user has a particular privilege on a language, given the language OID as a direct parameter.

## Definition

```c
Datum
has_language_privilege_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the most optimized variant of language privilege checking, combining the efficiency of OID-based language lookup with the convenience of assuming the current user. It takes only two arguments: a language OID and privilege type (text), then returns a boolean indicating whether the current user has the requested privilege. Like the name_id variant, it uses object_aclcheck_ext for explicit missing object detection and returns NULL when the language OID doesn't exist.

## Parameters / Member Variables
-  (Oid): The OID of the procedural language to check privileges for
-  (text): The privilege type being checked (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extract OID argument from function call
  - PG_GETARG_TEXT_PP: Extract text argument from function call
  - [GetUserId](../G/GetUserId.md): Get the OID of the current user
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Convert privilege string to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Perform privilege check with missing object detection
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Most efficient variant: no username lookup, no language name lookup required
- Assumes current_user, eliminating the need to specify a username parameter
- Uses object_aclcheck_ext for explicit missing object detection
- Explicitly returns NULL if the language OID doesn't exist
- Part of PostgreSQL's access control system for procedural languages
- All has_language_privilege variants are named 'has_language_privilege' at the SQL level
- Located in src/backend/utils/adt/acl.c:3685-3712