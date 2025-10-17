# regcollationin

## Location
[src/backend/utils/adt/regproc.c:1026-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1026-L1067)

## Overview
Converts a collation name string (or numeric OID) to its corresponding collation OID, with support for schema-qualified names and special values.

## Definition

```c
Datum
regcollationin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the text input function for the  data type. It converts various string representations of collations into their internal OID representation. The function handles multiple input formats to provide flexibility in referencing collations.

The function processes input in the following priority order:
1. Special value "-" representing unknown/invalid collation (returns OID 0)
2. Numeric OID strings (for symmetry with output function)
3. Collation names, which can be simple names or schema-qualified names

For named collations, the function:
- Parses schema-qualified names (e.g., "schema.collation")  
- Searches for the collation in the current database's search path
- Validates that the collation exists and is compatible with the database encoding
- Returns appropriate error messages for non-existent collations

The function includes bootstrap mode restrictions since full catalog lookups are not available during database initialization.

## Parameters / Member Variables
- Input:  (char*) - String containing collation name, schema.name, numeric OID, or "-"
- Input:  (Node*) - Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string argument from function call
  -  - Handle special "-" value and numeric OID parsing
  -  - Return OID result
  -  - Check if in bootstrap initialization mode
  -  - Parse schema-qualified names
  -  - Look up collation OID by name
  -  - Validate OID result
  -  - Return with error context
  -  - Convert name list to string for error messages
  -  - Get current database encoding name

- Called from (representative examples):
  -  - Conversion function using regcollationin

## Notes and Other Information
- Accepts "-" as special value representing unknown collation (OID 0)
- Supports both simple names and schema-qualified names for collation lookup
- Validates collation compatibility with database encoding
- Provides descriptive error messages including encoding information
- Restricted functionality in bootstrap mode - only accepts numeric OIDs
- Part of the regcollation type I/O function set for PostgreSQL's collation registry type
- Error handling includes soft error support via error context parameter
- Search path rules apply when resolving unqualified collation names
- Encoding compatibility is enforced at lookup time

## Simplified Source

```c
Datum
regcollationin(PG_FUNCTION_ARGS)
{
    char *collation_name_or_oid = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;
    List *names;

    // Handle special "-" value or numeric OID
    if (parseDashOrOid(collation_name_or_oid, &result, escontext))
        PG_RETURN_OID(result);

    // Bootstrap mode restriction
    if (IsBootstrapProcessingMode())
        elog(ERROR, "regcollation values must be OIDs in bootstrap mode");

    // Parse collation name (potentially schema-qualified)
    names = stringToQualifiedNameList(collation_name_or_oid, escontext);
    if (names == NIL)
        PG_RETURN_NULL();

    // Look up collation OID
    result = get_collation_oid(names, true);

    if (!OidIsValid(result))
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("collation \"%s\" for encoding \"%s\" does not exist",
                        NameListToString(names), GetDatabaseEncodingName())));

    PG_RETURN_OID(result);
}
```