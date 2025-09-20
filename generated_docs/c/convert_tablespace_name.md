# convert_tablespace_name

## Location
[src/backend/utils/adt/acl.c:4367-4378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4367-L4378)

## Overview
Converts a tablespace name expressed as a text string to its corresponding tablespace OID for use in tablespace privilege checking functions.

## Definition

```c
static Oid
convert_tablespace_name(text *tablespacename)
```
## Detailed Description
This is a utility function within the tablespace privilege checking system that takes a tablespace name in PostgreSQL's internal text format and converts it to the corresponding tablespace Object Identifier (OID). The function serves as a bridge between string-based tablespace names and the internal OID-based representation used throughout the PostgreSQL system. It performs the conversion by first extracting a null-terminated C string from the PostgreSQL text type, then using the system catalog lookup function to find the corresponding OID.

## Parameters / Member Variables
- : A PostgreSQL text type containing the name of the tablespace to look up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
- Called from (representative examples):
  - [has_tablespace_privilege_name_name](../h/has_tablespace_privilege_name_name.md)
  - [has_tablespace_privilege_name](../h/has_tablespace_privilege_name.md)
  - [has_tablespace_privilege_id_name](../h/has_tablespace_privilege_id_name.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the acl.c compilation unit
- The function will raise an error if the tablespace name doesn't exist (due to the false parameter passed to get_tablespace_oid)
- Part of the has_tablespace_privilege family of functions that check tablespace access permissions
- The function assumes the input text is valid and non-null