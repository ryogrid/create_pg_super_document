# has_language_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 3605 - 3630

## Overview
This function checks whether a specified user has a particular privilege on a language, given both the username and language name as text parameters.

## Definition


## Detailed Description
This function is one of the PostgreSQL SQL-callable privilege checking functions for procedural languages. It takes three arguments: a username (Name type), a language name (text), and a privilege type (text), then returns a boolean indicating whether the specified user has the requested privilege on the specified language. The function performs OID resolution for both the user and language, then delegates the actual privilege checking to the object_aclcheck system.

## Parameters / Member Variables
-  (Name): The name of the user/role whose privileges are being checked
-  (text): The name of the procedural language to check privileges for  
-  (text): The privilege type being checked (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extract Name argument from function call
  - PG_GETARG_TEXT_PP: Extract text arguments from function call
  - get_role_oid_or_public: Convert username to role OID
  - [convert_language_name](../c/convert_language_name.md): Convert language name to language OID
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Convert privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Perform the actual privilege check
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- This is one of several variants of has_language_privilege functions that differ in their parameter types (name vs OID)
- All variants are named 'has_language_privilege' at the SQL level
- Returns NULL if the language object doesn't exist
- Part of PostgreSQL's access control system for procedural languages
- Located in src/backend/utils/adt/acl.c:3605-3630