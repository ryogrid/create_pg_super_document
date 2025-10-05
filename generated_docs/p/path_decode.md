# path_decode

## Location
[src/backend/utils/adt/geo_ops.c:266-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L266-L339)

## Overview
A comprehensive function that parses sequences of coordinate pairs from string representations, handling both open and closed path formats for various PostgreSQL geometric data types.

## Definition
```c
static bool
path_decode(char *str, bool opentype, int npts, Point *p,
            bool *isopen, char **endptr_p,
            const char *type_name, const char *orig_string,
            Node *escontext)
```

## Detailed Description
The `path_decode` function is a sophisticated parser that handles sequences of coordinate pairs for geometric data types including paths, polygons, boxes, line segments, and lines. It can parse both open paths (using square brackets `[...]`) and closed paths (using parentheses `(...)` or double parentheses `((...))`) depending on the input format and type requirements.

The function intelligently detects delimiter patterns, manages nesting depth for complex geometric constructs, and parses each coordinate pair using `pair_decode`. It supports partial parsing by reporting the stopping point and provides comprehensive error handling with descriptive messages.

## Parameters / Member Variables
- `str`: Input string containing the sequence of coordinate pairs to be parsed
- `opentype`: Boolean flag indicating whether open path format (square brackets) is allowed for this geometric type  
- `npts`: Number of coordinate pairs expected to be parsed from the input
- `p`: Pointer to an array of Point structures where parsed coordinates will be stored
- `isopen`: Output parameter set to true if an open path format was detected, false for closed format
- `endptr_p`: Optional pointer to a char pointer that will be set to the character following the parsed sequence (NULL if full string parsing is required)
- `type_name`: Name of the geometric type being parsed (used for error messages)
- `orig_string`: Original input string (used for error context in messages) 
- `escontext`: Error handling context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - `[pair_decode](pair_decode.md)`: Used to parse each individual coordinate pair in the sequence
  - `LDELIM_EP`: Left delimiter for open paths (square bracket)
  - `LDELIM`: Left delimiter for closed paths (parenthesis)
  - `DELIM`: Coordinate pair separator (comma)
  - `RDELIM`: Right delimiter for closed paths (parenthesis)  
  - `RDELIM_EP`: Right delimiter for open paths (square bracket)
  - `ereturn`: Error return macro for soft error handling
  - `[Point](../P/Point.md)`: Geometric data structure for storing coordinate pairs
  - `isspace`: Standard library function for whitespace detection
  - `strrchr`: Standard library function for finding last occurrence of character
- Called from (representative examples):
  - `[path_in](path_in.md)`: For parsing path input strings like "[(1,2),(3,4)]" or "((1,2),(3,4))"
  - `[poly_in](poly_in.md)`: For parsing polygon input strings like "((0,0),(1,0),(1,1),(0,1))"
  - `[box_in](../b/box_in.md)`: For parsing box input strings like "(1,2),(3,4)"
  - `[lseg_in](../l/lseg_in.md)`: For parsing line segment coordinates
  - `[line_in](../l/line_in.md)`: For parsing line coordinates

## Notes and Other Information
- This is a static function, only accessible within the geo_ops.c file
- Returns `true` on successful parsing, `false` if an error occurred
- Handles both open path format `[...]` and closed path formats `(...)` or `((...))` 
- Uses sophisticated delimiter depth tracking to handle nested parentheses correctly
- Supports partial string parsing with endptr_p parameter for compound geometric types
- Uses PostgreSQL's soft error handling mechanism for graceful error recovery
- The function is central to PostgreSQL's geometric data type parsing infrastructure
- Manages whitespace appropriately between coordinate pairs and around delimiters  
- Error messages include the original input string and type name for better user feedback
- The nesting depth logic correctly handles various geometric data type format variations
- Part of PostgreSQL's comprehensive geometric data type system that maintains consistent parsing behavior across multiple geometric types

## Simplified Source

```c
static bool path_decode(char *str, bool opentype, int npts, Point *p,
                       bool *isopen, char **endptr_p,
                       const char *type_name, const char *orig_string,
                       Node *escontext) {
    int depth = 0;
    char *cp;
    int i;

    // Skip leading whitespace
    while (isspace((unsigned char) *str))
        str++;

    // Check for open path format [...]
    if ((*isopen = (*str == LDELIM_EP))) {
        if (!opentype)  // open delimiter not allowed
            goto fail;
        depth++;
        str++;
    }
    // Check for closed path format (...) or ((...))
    else if (*str == LDELIM) {
        cp = (str + 1);
        while (isspace((unsigned char) *cp))
            cp++;
        // Handle nested parentheses for formats like ((...))
        if (*cp == LDELIM || strrchr(str, LDELIM) == str) {
            depth++;
            str = cp;
        }
    }

    // Parse coordinate pairs
    for (i = 0; i < npts; i++) {
        if (!pair_decode(str, &(p->x), &(p->y), &str, type_name, orig_string, escontext))
            return false;
        if (*str == DELIM)  // skip comma separator
            str++;
        p++;
    }

    // Match closing delimiters
    while (depth > 0) {
        if (*str == RDELIM || (*str == RDELIM_EP && *isopen && depth == 1)) {
            depth--;
            str++;
            while (isspace((unsigned char) *str))
                str++;
        } else {
            goto fail;
        }
    }

    // Set end pointer or validate end of string
    if (endptr_p)
        *endptr_p = str;
    else if (*str != '\0')
        goto fail;

    return true;

fail:
    ereturn(escontext, false,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"",
                    type_name, orig_string)));
}
```