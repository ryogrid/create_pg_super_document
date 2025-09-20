# has_language_privilege_name

## Location
[src/backend/utils/adt/acl.c:3631-3654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3631-L3654)

## Overview
This function checks whether the current user has a particular privilege on a language, given the language name as a text parameter.

## Definition

```c
Datum
has_language_privilege_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a simplified variant of the has_language_privilege family that assumes the current user as the subject of the privilege check. It takes two arguments: a language name (text) and a privilege type (text), then returns a boolean indicating whether the current user has the requested privilege on the specified language. The function uses GetUserId() to automatically determine the current user's OID and performs the same privilege checking logic as other variants.

## Parameters / Member Variables
-  (text): The name of the procedural language to check privileges for
-  (text): The privilege type being checked (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extract text arguments from function call
  - [GetUserId](../G/GetUserId.md): Get the OID of the current user
  - [convert_language_name](../c/convert_language_name.md): Convert language name to language OID
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Convert privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Perform the actual privilege check
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- This variant assumes current_user, eliminating the need to specify a username parameter
- Returns NULL if the language object doesn't exist
- Part of PostgreSQL's access control system for procedural languages
- All has_language_privilege variants are named 'has_language_privilege' at the SQL level
- Located in src/backend/utils/adt/acl.c:3631-3654