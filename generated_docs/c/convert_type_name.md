# convert_type_name

## Location
[src/backend/utils/adt/acl.c:4566-4586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4566-L4586)

## Overview
Converts a type name expressed as text to its corresponding object identifier (OID).

## Definition
```c
static Oid convert_type_name(text *typename)
```

## Detailed Description
This static function serves as a support routine for the has_type_privilege family of functions. It takes a PostgreSQL text object containing a type name and converts it to the corresponding type OID. The function uses PostgreSQL's regtype input function to perform the name-to-OID conversion, which handles type name parsing and namespace resolution. If the type name is invalid or the type does not exist, the function reports an error rather than returning an invalid OID.

## Parameters / Member Variables
- `typename`: A PostgreSQL text object containing the name of the type to look up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring: Converts PostgreSQL text to C string
  - DirectFunctionCall1: Calls PostgreSQL internal function directly
  - [regtypein](../r/regtypein.md): PostgreSQL function that converts type name string to OID
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - [DatumGetObjectId](../D/DatumGetObjectId.md): Extracts OID from Datum result
  - OidIsValid: Checks if OID is valid
  - ereport: Reports error if type does not exist
- Called from (representative examples):
  - [has_type_privilege_name_name](../h/has_type_privilege_name_name.md): Privilege check with role name and type name
  - [has_type_privilege_name](../h/has_type_privilege_name.md): Privilege check with current user and type name  
  - [has_type_privilege_id_name](../h/has_type_privilege_id_name.md): Privilege check with role OID and type name

## Notes and Other Information
- This is a static (internal) function, not exposed outside acl.c
- Uses PostgreSQL's regtype system to handle complex type names and namespace resolution
- Reports ERRCODE_UNDEFINED_OBJECT error if the type name is invalid
- Part of the type privilege checking infrastructure
- Handles schema-qualified type names through the regtype mechanism
- Located in src/backend/utils/adt/acl.c:4566-4586