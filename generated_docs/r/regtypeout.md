# regtypeout

## Location
[src/backend/utils/adt/regproc.c:1247-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1247-L1294)

## Overview
Converts a type OID to its corresponding textual type name representation for output purposes.

## Definition

```c
Datum
regtypeout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is PostgreSQL's output function for the regtype data type. It takes a type OID (Object Identifier) and converts it to a human-readable string representation of the type name. The function handles several special cases:

1. **Invalid OID**: Returns "-" for InvalidOid
2. **Valid type OID**: Looks up the type in pg_type catalog and returns its formatted name
3. **Bootstrap mode**: Returns the simple type name without namespace qualification
4. **Non-existent type**: Returns the numeric OID as a string

The function uses the system catalog lookup to find type information and employs  for proper type name formatting in normal operation mode.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - First argument (index 0): OID of the type to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to return a C string from a PostgreSQL function
  - : Structure representing a row in the pg_type catalog
  - : Checks if PostgreSQL is in bootstrap mode
  - : Maximum length for PostgreSQL names
  - : Searches system cache for tuple by single key
  - : Formats type name with proper namespace qualification
  - : PostgreSQL string duplication function
  - : PostgreSQL memory allocation function
- Called from (representative examples):
  - No direct references found in the codebase (likely called by PostgreSQL's type system)

## Notes and Other Information
- This is the output function for the regtype data type
- Handles bootstrap mode differently by skipping namespace qualification
- Returns numeric OID string for non-existent types rather than throwing an error
- Uses system catalog caching for efficient type lookup
- Part of PostgreSQL's regtype type system implementation
- Located in src/backend/utils/adt/regproc.c

## Simplified Source

```c
Datum regtypeout(PG_FUNCTION_ARGS) {
    Oid typid = PG_GETARG_OID(0);
    char *result;

    // Handle invalid OID
    if (typid == InvalidOid) {
        result = pstrdup("-");
        PG_RETURN_CSTRING(result);
    }

    // Look up type in system catalog
    HeapTuple typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(typetup)) {
        Form_pg_type typeform = (Form_pg_type) GETSTRUCT(typetup);

        // Bootstrap mode: return simple type name
        if (IsBootstrapProcessingMode()) {
            char *typname = NameStr(typeform->typname);
            result = pstrdup(typname);
        }
        // Normal mode: use formatted type name with namespace
        else {
            result = format_type_be(typid);
        }

        ReleaseSysCache(typetup);
    }
    else {
        // Type not found: return numeric OID
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", typid);
    }

    PG_RETURN_CSTRING(result);
}
```