# convert_function_name

## Location
[src/backend/utils/adt/acl.c:3556-3576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3556-L3576)

## Overview
Converts a function name given as text into its corresponding function OID for privilege checking operations.

## Definition
```c
static Oid convert_function_name(text *functionname)
```

## Detailed Description
This is a static helper function used by the has_function_privilege family of functions to convert a textual function name into its corresponding Object ID (OID). The function uses PostgreSQL's regprocedurein input function to parse the function name and resolve it to an OID. If the function name cannot be resolved to a valid function, it raises an error rather than returning an invalid OID.

## Parameters / Member Variables
- `functionname`: A text pointer containing the name of the function to be resolved to an OID

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)(): Converts PostgreSQL text type to C string
  - DirectFunctionCall1(): Calls a PostgreSQL function with one argument
  - [regprocedurein](../r/regprocedurein.md)(): PostgreSQL input function that converts string to regprocedure
  - [DatumGetObjectId](../D/DatumGetObjectId.md)(): Extracts OID from a Datum
  - [CStringGetDatum](../C/CStringGetDatum.md)(): Converts C string to Datum
  - OidIsValid(): Checks if an OID is valid
  - ereport(): Reports errors
- Called from (representative examples):
  - [has_function_privilege_name_name](../h/has_function_privilege_name_name.md)
  - [has_function_privilege_name](../h/has_function_privilege_name.md)
  - [has_function_privilege_id_name](../h/has_function_privilege_id_name.md)

## Notes and Other Information
- This is a static function, only visible within the acl.c file
- Raises ERROR with ERRCODE_UNDEFINED_FUNCTION if the function doesn't exist
- Uses regprocedurein which supports function signatures (name with parameter types)
- Part of the support routines for the has_function_privilege family of functions
- Located in src/backend/utils/adt/acl.c:3556-3576