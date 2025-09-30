# get_transform_oid

## Location
[src/backend/commands/functioncmds.c:2019-2042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L2019-L2042)

## Overview
Looks up a transform OID based on a given type OID and language OID, providing controlled error handling for missing transforms.

## Definition

```c
Oid
get_transform_oid(Oid type_id, Oid lang_id, bool missing_ok)
```
## Detailed Description
This function searches the PostgreSQL system catalog for a transform that matches the specified data type and procedural language combination. Transforms define how data types are converted between SQL and procedural language representations (such as PL/Python or PL/Perl). The function uses the system cache to efficiently look up the transform OID from the pg_transform catalog table using the TRFTYPELANG cache.

The function provides flexible error handling through the missing_ok parameter - it can either throw a descriptive error when no matching transform is found, or silently return InvalidOid to allow the caller to handle the absence gracefully.

## Parameters / Member Variables
- `type_id`: The OID of the data type for which to find a transform
- `lang_id`: The OID of the procedural language for which to find a transform  
- `missing_ok`: If false, throws an error when transform is not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2 (system cache lookup)
  - [format_type_be](../f/format_type_be.md) (error message formatting)
  - [get_language_name](get_language_name.md) (error message formatting)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [CreateFunction](../C/CreateFunction.md)

## Notes and Other Information
- Uses the TRFTYPELANG system cache for efficient lookups
- Part of PostgreSQL's extensible type system supporting procedural languages
- Returns InvalidOid when transform doesn't exist and missing_ok is true
- Error messages include human-readable type and language names for better diagnostics
- Critical for supporting data type conversions in stored procedures and functions written in procedural languages

## Simplified Source

```c
Oid
get_transform_oid(Oid type_id, Oid lang_id, bool missing_ok) {
    Oid oid;

    // Look up transform in system cache
    oid = GetSysCacheOid2(TRFTYPELANG, Anum_pg_transform_oid,
                          ObjectIdGetDatum(type_id),
                          ObjectIdGetDatum(lang_id));

    // Handle not found case
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR, "transform for type %s language \"%s\" does not exist",
                format_type_be(type_id),
                get_language_name(lang_id, false));

    return oid;
}
```