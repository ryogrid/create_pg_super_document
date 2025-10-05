# path_in

## Location
[src/backend/utils/adt/geo_ops.c:1402-1473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1402-L1473)

## Overview
Parses a string representation of a geometric path and converts it into PostgreSQL's internal PATH data structure.

## Definition

```c
Datum
path_in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the input conversion routine for the PATH geometric data type in PostgreSQL. It parses various string representations of paths and creates the corresponding internal PATH structure. The function supports multiple input formats including both open paths (polylines) and closed paths (polygons).

Supported input formats:
-   (parentheses for closed/open paths)
-   (brackets for closed/open paths)  
-    (simple comma-separated)
-       (flat coordinate list)
-   (legacy format)

The parsing process:
1. Counts coordinate pairs using pair_count()
2. Allocates memory for the PATH structure
3. Delegates actual parsing to path_decode()
4. Sets the closed flag based on parsing results
5. Validates the complete input string

## Parameters / Member Variables
- : PostgreSQL function argument macro that provides access to:
  - Argument 0: C string containing the path representation
  - fcinfo->context: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (retrieves string argument)
  - [pair_count](pair_count.md) (counts coordinate pairs in string)
  - ereturn (soft error return)
  - [palloc](palloc.md) (memory allocation)
  - SET_VARSIZE (sets variable-length type size)
  - [path_decode](path_decode.md) (parses path coordinates)
  - PG_RETURN_PATH_P (returns PATH result)
  - isspace (character classification)
  - strrchr (string search)
- Constants used:
  - LDELIM, RDELIM (delimiter characters)
  - ERRCODE_INVALID_TEXT_REPRESENTATION
  - ERRCODE_PROGRAM_LIMIT_EXCEEDED
- Types used:
  - [PATH](../P/PATH.md) (geometric path type)
  - [Node](../N/Node.md) (error context)
  - Datum (PostgreSQL data type)
- Called from:
  - No direct references found (likely called via SQL type conversion interface)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1402-1473
- Part of PostgreSQL's type input/output system for geometric types
- Includes integer overflow protection when calculating memory requirements
- Supports soft error handling through error context
- Sets the closed flag based on bracket vs parenthesis delimiters in some formats
- Initializes dummy field to prevent instability in unused padding bytes
- Validates complete input consumption to detect malformed strings
- The actual coordinate parsing logic is delegated to the path_decode helper function

## Simplified Source

```c
Datum path_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    PATH *path;
    bool isopen;
    char *s;
    int npts, size, base_size, depth = 0;

    // Count coordinate pairs
    if ((npts = pair_count(str, ',')) <= 0)
        ereturn(escontext, (Datum) 0, /* invalid input syntax error */);

    // Skip whitespace and handle leading parenthesis
    s = str;
    while (isspace((unsigned char) *s)) s++;
    if ((*s == LDELIM) && (strrchr(s, LDELIM) == s)) {
        s++;
        depth++;
    }

    // Calculate size and check for overflow
    base_size = sizeof(path->p[0]) * npts;
    size = offsetof(PATH, p) + base_size;
    if (base_size / npts != sizeof(path->p[0]) || size <= base_size)
        ereturn(escontext, (Datum) 0, /* too many points error */);

    // Allocate and initialize PATH structure
    path = (PATH *) palloc(size);
    SET_VARSIZE(path, size);
    path->npts = npts;

    // Parse coordinates using helper function
    if (!path_decode(s, true, npts, &(path->p[0]), &isopen, &s, "path", str, escontext))
        PG_RETURN_NULL();

    // Validate closing parenthesis and end of string
    if (depth >= 1) {
        if (*s++ != RDELIM)
            ereturn(escontext, (Datum) 0, /* invalid syntax error */);
        while (isspace((unsigned char) *s)) s++;
    }
    if (*s != '\0')
        ereturn(escontext, (Datum) 0, /* invalid syntax error */);

    // Set path properties
    path->closed = (!isopen);
    path->dummy = 0;  // Prevent padding instability

    PG_RETURN_PATH_P(path);
}
```