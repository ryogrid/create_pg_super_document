# pg_replication_origin_oid

## Location
[src/backend/replication/logical/origin.c:1329-1349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1329-L1349)

## Overview
SQL-callable function that returns the OID (object identifier) of a replication origin given its name, providing a way to look up origin identifiers from SQL.

## Definition
```c
Datum pg_replication_origin_oid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function wrapper for retrieving replication origin OIDs by name. It takes a text parameter containing the origin name, validates prerequisites, converts the text to a C string, and uses the internal `replorigin_by_name()` function to perform the catalog lookup. If the origin exists, it returns the OID; otherwise, it returns NULL. The function can be called from SQL as `SELECT pg_replication_origin_oid('origin_name')`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: `text` - Name of the replication origin to look up (converted internally to C string)

## Dependencies
- Functions called/Symbols referenced:
  - `RepOriginId` - Type definition for replication origin identifiers
  - `[replorigin_check_prerequisites](../r/replorigin_check_prerequisites.md)` - Validates that replication origins can be accessed (not in recovery, proper configuration)
  - `[text_to_cstring](../t/text_to_cstring.md)` - Converts PostgreSQL text datum to null-terminated C string
  - `[replorigin_by_name](../r/replorigin_by_name.md)` - Performs the actual origin lookup in system catalog with `missing_ok=true`
  - [pfree](pfree.md) - Frees allocated memory for the converted string
  - `OidIsValid` - Checks if the returned OID is valid
  - `PG_RETURN_OID` - Returns OID result to PostgreSQL function call framework
  - `PG_RETURN_NULL` - Returns NULL result when origin not found
- Called from (representative examples):
  - SQL interface (no direct C callers found)

## Notes and Other Information
- Uses `missing_ok=true` when calling `replorigin_by_name`, so it gracefully handles non-existent origins by returning NULL instead of throwing an error
- Performs prerequisite checks to ensure replication origins can be accessed 
- Automatically handles memory management for the converted origin name string
- Part of PostgreSQL's logical replication origin management system
- Provides a safe way to check for origin existence and get its identifier from SQL
- Located in `src/backend/replication/logical/origin.c:1329-1349`

## Simplified Source

```c
Datum
pg_replication_origin_oid(PG_FUNCTION_ARGS)
{
    char *name;
    RepOriginId roident;

    // Check prerequisites (not in recovery, proper configuration)
    replorigin_check_prerequisites(false, false);

    // Convert text argument to C string
    name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));

    // Look up origin by name (missing_ok=true)
    roident = replorigin_by_name(name, true);

    pfree(name);

    // Return OID if found, NULL otherwise
    if (OidIsValid(roident))
        PG_RETURN_OID(roident);
    PG_RETURN_NULL();
}
```