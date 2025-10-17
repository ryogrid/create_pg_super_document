# parseDashOrOid

## Location
[src/backend/utils/adt/regproc.c:1868-1894](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1868-L1894)

## Overview
A utility function that parses a string as either a dash ("-") representing InvalidOid or a numeric OID, providing a convenient way to handle both null/invalid OID representations and valid numeric OIDs in PostgreSQL's regtype system.

## Definition

```c
static bool
parseDashOrOid(char *string, Oid *result, Node *escontext)
```
## Detailed Description
The  function extends the functionality of  by adding special handling for the dash character ("-"). This function is part of PostgreSQL's regtype input parsing system and serves as a helper for various reg* type input functions. It provides a standardized way to represent InvalidOid using a human-readable dash symbol, which is commonly used in PostgreSQL's system catalogs and dumps to indicate null or invalid object references.

The function first checks if the input string is exactly "-", and if so, sets the result to InvalidOid and returns true. Otherwise, it delegates to  to handle standard numeric OID parsing.

## Parameters / Member Variables
- `*string`: Input C string to be parsed, expected to be either "-" or a numeric OID representation
- `*result`: Pointer to Oid where the parsed result will be stored (InvalidOid for "-", or the numeric OID value)
- `*escontext`: Error context node for soft error handling, allowing errors to be captured rather than thrown as exceptions
## Dependencies
- Functions called/Symbols referenced:
  - [parseNumericOid](parseNumericOid.md)
  - strcmp (standard C library)
  - InvalidOid (PostgreSQL constant)
- Called from (representative examples):
  - [regprocin](../r/regprocin.md)
  - [regprocedurein](../r/regprocedurein.md)
  - [regclassin](../r/regclassin.md)
  - [regcollationin](../r/regcollationin.md)
  - [regtypein](../r/regtypein.md)
  - [regconfigin](../r/regconfigin.md)
  - [regdictionaryin](../r/regdictionaryin.md)
  - [regrolein](../r/regrolein.md)
  - [regnamespacein](../r/regnamespacein.md)

## Notes and Other Information
- This function is static, meaning it's only accessible within the regproc.c file
- The dash ("-") representation is commonly seen in pg_dump output and system catalog representations
- Returns true if parsing succeeds (either dash or valid numeric OID), false if the string doesn't match either pattern
- Part of PostgreSQL's type input/output system for object identifier types (reg* types)
- The escontext parameter supports PostgreSQL's soft error handling mechanism introduced for better error reporting in input functions

## Simplified Source

```c
static bool parseDashOrOid(char *string, Oid *result, Node *escontext) {
    // Handle dash as InvalidOid
    if (strcmp(string, "-") == 0) {
        *result = InvalidOid;
        return true;
    }

    // Delegate to numeric OID parsing
    return parseNumericOid(string, result, escontext);
}
```