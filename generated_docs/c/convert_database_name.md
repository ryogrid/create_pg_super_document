# convert_database_name

## Location
[src/backend/utils/adt/acl.c:3150-3161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3150-L3161)

## Overview
Converts a database name (text) to its corresponding database OID for use in privilege checking operations.

## Definition
```c
static Oid convert_database_name(text *databasename)
```

## Detailed Description
This is a static helper function used by the has_database_privilege family of functions. It takes a PostgreSQL text object containing a database name and converts it to the corresponding database OID by looking up the name in the system catalogs. The function performs the conversion by first extracting a C string from the text object, then using the standard PostgreSQL function to resolve the database name to its OID.

## Parameters / Member Variables
- `databasename`: A PostgreSQL text object containing the name of the database to look up

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md): Converts PostgreSQL text type to C string
  - [get_database_oid](../g/get_database_oid.md): Looks up database OID by name (with error if not found)
- Called from (representative examples):
  - [has_database_privilege_name_name](../h/has_database_privilege_name_name.md): User name + database name variant
  - [has_database_privilege_name](../h/has_database_privilege_name.md): Current user + database name variant  
  - [has_database_privilege_id_name](../h/has_database_privilege_id_name.md): User OID + database name variant

## Notes and Other Information
- This is a static function, only accessible within the same source file
- The second parameter to get_database_oid is false, meaning it will raise an error if the database does not exist
- Part of the support routines for the has_database_privilege family of functions
- Located in src/backend/utils/adt/acl.c:3150-3161