# has_language_privilege_id_name

## Location
src/backend/utils/adt/acl.c: 3713 - 3735

## Overview
This function checks whether a specified user has a particular privilege on a language, given the user's role OID and the language name as parameters.

## Definition


## Detailed Description
This function provides a mixed approach to privilege checking where the user is specified by role OID (avoiding username lookup) but the language is specified by name (requiring name-to-OID conversion). It takes a role OID, language name (text), and privilege type (text), then returns a boolean indicating whether the specified user has the requested privilege on the language. This variant uses the standard object_aclcheck function rather than the extended version.

## Parameters / Member Variables
-  (Oid): The OID of the user/role whose privileges are being checked
-  (text): The name of the procedural language to check privileges for
-  (text): The privilege type being checked (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extract OID argument from function call
  - PG_GETARG_TEXT_PP: Extract text arguments from function call
  - [convert_language_name](../c/convert_language_name.md): Convert language name to language OID
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Convert privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Perform the actual privilege check
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Optimizes user lookup (takes OID directly) but requires language name conversion
- Uses standard object_aclcheck rather than object_aclcheck_ext
- Part of PostgreSQL's access control system for procedural languages
- All has_language_privilege variants are named 'has_language_privilege' at the SQL level
- Returns NULL if the language object doesn't exist (via convert_language_name behavior)
- Located in src/backend/utils/adt/acl.c:3713-3735