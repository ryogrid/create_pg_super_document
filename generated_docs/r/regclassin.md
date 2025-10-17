# regclassin

## Location
[src/backend/utils/adt/regproc.c:882-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L882-L924)

## Overview
Converts a class name (table, view, sequence, etc.) to its corresponding OID, serving as the input function for the regclass data type.

## Definition

```c
Datum
regclassin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is an input function for the  data type in PostgreSQL. It accepts either a class name (potentially schema-qualified) or a numeric OID and converts it to the appropriate relation OID. The function handles several input formats:

1. **Dash ("-")**: Represents an unknown or invalid OID (returns 0)
2. **Numeric OID**: Directly parsed and returned for symmetry with output routines
3. **Class name**: Looked up in the system catalogs, potentially schema-qualified

The function performs name resolution using the current search path and validates that the relation exists. It handles both simple names like "mytable" and schema-qualified names like "public.mytable". The function includes special handling for bootstrap mode where only numeric OIDs are accepted.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - Argument 0:  - The class name or OID string to convert
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string argument from function call
  -  - Handle dash or numeric OID parsing  
  -  - Return OID value from function
  -  - Check if in bootstrap mode
  -  - Parse potentially schema-qualified name
  -  - Create RangeVar from name list
  -  - Look up relation OID from RangeVar
  -  - Validate OID
  -  - Return error with context
  -  - Convert name list back to string for error messages

- Called from (representative examples):
  -  - Uses this function for class name to OID conversion

## Notes and Other Information
- Accepts both simple and schema-qualified relation names (e.g., "mytable" or "public.mytable")
- Uses current search path for name resolution when schema is not specified
- In bootstrap mode, only numeric OIDs are accepted, not names
- The function does not lock the relation during lookup for performance reasons
- Provides detailed error messages when relations don't exist
- Part of the regtype family of input/output functions for database object references
- Returns 0 (InvalidOid) for dash input, following PostgreSQL conventions for "unknown" values

## Simplified Source

```c
Datum regclassin(PG_FUNCTION_ARGS) {
    char *class_name_or_oid = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;
    List *names;

    // Handle "-" (unknown) or numeric OID input
    if (parseDashOrOid(class_name_or_oid, &result, escontext))
        PG_RETURN_OID(result);

    // Bootstrap mode only accepts numeric OIDs
    if (IsBootstrapProcessingMode())
        elog(ERROR, "regclass values must be OIDs in bootstrap mode");

    // Parse potentially schema-qualified class name
    names = stringToQualifiedNameList(class_name_or_oid, escontext);
    if (names == NIL)
        PG_RETURN_NULL();

    // Look up relation OID without locking (performance optimization)
    result = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true);

    if (!OidIsValid(result))
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("relation \"%s\" does not exist",
                        NameListToString(names))));

    PG_RETURN_OID(result);
}
```