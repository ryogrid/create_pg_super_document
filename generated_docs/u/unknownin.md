# unknownin

## Location
[src/backend/utils/adt/varlena.c:634-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L634-L645)

## Overview
The  function converts a C-style string to internal representation for the  data type, which maintains the same representation as the input cstring.

## Definition

```c
Datum
unknownin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL data type input function that handles the conversion of external string representations into PostgreSQL's internal format for the  data type. Unlike most other input functions, the  type maintains the exact same representation as a C string internally. The function simply duplicates the input string using  to create a separate copy that PostgreSQL can manage independently. This type is used internally by PostgreSQL for literals and expressions whose type cannot be determined at parse time.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: A C-style string () to be converted to unknown format

## Dependencies
- Functions called/Symbols referenced:
  - : Macro for returning a C string from a PostgreSQL function
  - : PostgreSQL memory allocation function that duplicates a string (implicitly used)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is registered as the input function for the  data type in PostgreSQL's type system
- The  type is special in that its internal representation is identical to cstring
- Uses  to create a memory-managed copy of the input string
- The  type is primarily used during query parsing and planning when the actual type of a literal or expression cannot be determined immediately
- Part of PostgreSQL's type system infrastructure for handling type resolution
- Located in src/backend/utils/adt/varlena.c

## Simplified Source

```c
Datum unknownin(PG_FUNCTION_ARGS) {
    // Get input C string
    char *str = PG_GETARG_CSTRING(0);

    // Return a duplicated copy (unknown type uses same representation as cstring)
    PG_RETURN_CSTRING(pstrdup(str));
}
``` 