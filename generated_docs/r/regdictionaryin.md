# regdictionaryin

## Location
[src/backend/utils/adt/regproc.c:1431-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1431-L1468)

## Overview
Converts a text search dictionary name string to its corresponding dictionary OID, handling both named and numeric input formats.

## Definition

```c
Datum
regdictionaryin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is part of PostgreSQL's regtype system for text search dictionaries. It parses input strings and converts them to dictionary OIDs. The function handles several input formats and scenarios:

1. **Special syntax "-"**: Returns  (0) to represent unknown/null dictionary
2. **Numeric OID**: Accepts and validates numeric OID strings
3. **Dictionary name**: Resolves qualified or unqualified dictionary names using the current search path
4. **Schema-qualified names**: Handles "schema.dictionary" format

The function performs comprehensive validation, checking that named dictionaries exist in the  system catalog and are accessible according to PostgreSQL's search path rules. It includes special handling for bootstrap mode where only numeric OIDs are accepted.

## Parameters / Member Variables
- **Input**: C string containing dictionary name, qualified name, numeric OID, or "-"
- **Return**:  containing the resolved dictionary OID
- **Error context**: Supports soft error reporting via error context

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string argument from function call
  -  - Handle "-" syntax and numeric OIDs
  -  - Return OID result
  -  - Check if in bootstrap mode
  -  - Parse qualified name strings
  -  - Look up dictionary OID by name
  -  - Validate OID values
  -  - Return error with context
  -  - Convert name list back to string for error messages
- Called from:
  - SQL type conversion system (indirectly)

## Notes and Other Information
- This function is the input counterpart to 
- Supports PostgreSQL's schema qualification and search path resolution
- Provides detailed error messages for non-existent dictionaries
- Bootstrap mode restriction ensures system stability during initialization
- Uses soft error handling to support contexts where errors should not abort transactions
- Part of the text search framework's type system integration

## Simplified Source

```c
Datum
regdictionaryin(PG_FUNCTION_ARGS)
{
    char *input = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;

    // Handle "-" or numeric OID input
    if (parseDashOrOid(input, &result, escontext)) {
        PG_RETURN_OID(result);
    }

    // Bootstrap mode only accepts numeric OIDs
    if (IsBootstrapProcessingMode()) {
        elog(ERROR, "regdictionary values must be OIDs in bootstrap mode");
    }

    // Parse qualified name and look up in catalog
    List *names = stringToQualifiedNameList(input, escontext);
    if (names == NIL) {
        PG_RETURN_NULL();
    }

    result = get_ts_dict_oid(names, true);

    if (!OidIsValid(result)) {
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("text search dictionary \"%s\" does not exist",
                        NameListToString(names))));
    }

    PG_RETURN_OID(result);
}
```