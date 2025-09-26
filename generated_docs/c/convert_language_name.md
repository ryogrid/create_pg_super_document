# convert_language_name

## Location
[src/backend/utils/adt/acl.c:3765-3776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3765-L3776)

## Overview
Converts a procedural language name from text format to its corresponding OID in the PostgreSQL system catalogs.

## Definition

```c
static Oid
convert_language_name(text *languagename)
```
## Detailed Description
This is a support function for the has_language_privilege family of functions. It takes a language name as a PostgreSQL text type and converts it to the corresponding language OID by looking up the language in the system catalogs. The function is static, indicating it's only used within the acl.c file. It uses PostgreSQL's standard text-to-cstring conversion and then calls get_language_oid to perform the actual lookup.

## Parameters / Member Variables
-  (text*): The name of the procedural language as a PostgreSQL text type

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md): Converts PostgreSQL text type to a C string
  - [get_language_oid](../g/get_language_oid.md): Looks up the language OID by name (with error if not found)
- Called from (representative examples):
  - [has_language_privilege_name_name](../h/has_language_privilege_name_name.md): Checks language privileges using role name and language name
  - [has_language_privilege_name](../h/has_language_privilege_name.md): Checks language privileges for current user using language name
  - [has_language_privilege_id_name](../h/has_language_privilege_id_name.md): Checks language privileges using role OID and language name

## Notes and Other Information
- This is a static helper function, not exposed outside of acl.c
- The second parameter to get_language_oid is false, meaning it will throw an error if the language doesn't exist
- Part of PostgreSQL's privilege checking infrastructure for procedural languages
- Located in src/backend/utils/adt/acl.c:3765-3776