# text_format_parse_format

## Location
[src/backend/utils/adt/varlena.c:5964-6040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5964-L6040)

## Overview
The  function parses the components of a printf-style format specifier, extracting argument positions, flags, width specifications, and other formatting parameters.

## Definition

```c
static const char *
text_format_parse_format(const char *start_ptr, const char *end_ptr,
						 int *argpos, int *widthpos,
						 int *flags, int *width)
```
## Detailed Description
This static helper function implements the core parsing logic for PostgreSQL's FORMAT() function format specifiers. It parses format specifiers following the SUS (Single UNIX Specification) printf format, handling the syntax: 

The function processes format specifiers in the following order:
1. **Argument position**: Optional numeric position followed by '$' (e.g., "2$" for 2nd argument)
2. **Flags**: Currently only supports minus flag ('-') for left-alignment
3. **Width specification**: Either direct numeric width or indirect width via '*' and optional position

Key parsing behaviors:
- Distinguishes between direct width values and argument position markers using the presence of '$'
- Supports indirect width specification using '*' with optional positional argument
- Validates that argument positions are >= 1 (0 is explicitly rejected)
- Maintains parsing invariants for safe string traversal

## Parameters / Member Variables
- `*start_ptr`: Pointer to the character after the initial '%' in the format specifier (input)
- `*end_ptr`: Pointer to the end of the format string (input, boundary check)
- `*argpos`: Parsed argument position for the value to be formatted, -1 if unspecified (output)
- `*widthpos`: Argument position for width value, 0 if next argument should be used, -1 if no width argument (output)
- `*flags`: Bitmask of formatting flags (currently only supports ) (output)
- `*width`: Direct width specification, 0 if width was omitted (output)
## Dependencies
- Functions called/Symbols referenced:
  -  - Parse numeric values from format string (called multiple times)
  -  - Macro for safe pointer advancement with bounds checking
  -  - Flag constant for left-alignment formatting
  - , , ,  - PostgreSQL error reporting system
- Called from (representative examples):
  -  - Main FORMAT() function implementation
  -  - [Variable](../V/Variable.md)-length string datum extraction

## Notes and Other Information
- Located in
- Static function, only accessible within the same compilation unit
- Returns pointer to the type character position (not consumed by this function)
- Implements comprehensive validation of format specifier syntax
- Argument positions must be >= 1 (PostgreSQL uses 1-based indexing, not 0-based)
- The '*' character indicates that width should be taken from a function argument
- Supports positional width arguments (e.g., "*2$" means width comes from 2nd argument)
- Maintains parsing invariant that at least one character remains available at function exit
- Part of the infrastructure supporting PostgreSQL's type-safe FORMAT() SQL function
- Error messages provide specific guidance about format specifier syntax requirements

## Simplified Source

```c
static const char *text_format_parse_format(const char *start_ptr, const char *end_ptr,
                                           int *argpos, int *widthpos,
                                           int *flags, int *width) {
    const char *cp = start_ptr;
    int n;

    // Initialize output parameters
    *argpos = -1;    // No specific argument position
    *widthpos = -1;  // No width argument
    *flags = 0;      // No formatting flags
    *width = 0;      // No direct width

    // Try to parse first number (could be argpos or width)
    if (text_format_parse_digits(&cp, end_ptr, &n)) {
        if (*cp != '$') {
            // Number without '$' is direct width specification
            *width = n;
            return cp;
        }
        // Number followed by '$' is argument position
        *argpos = n;
        if (n == 0)
            ereport(ERROR, "argument positions start from 1, not 0");
        ADVANCE_PARSE_POINTER(cp, end_ptr);
    }

    // Parse flags (currently only '-' for left-align)
    while (*cp == '-') {
        *flags |= TEXT_FORMAT_FLAG_MINUS;
        ADVANCE_PARSE_POINTER(cp, end_ptr);
    }

    // Parse width specification
    if (*cp == '*') {
        // Indirect width: get width value from an argument
        ADVANCE_PARSE_POINTER(cp, end_ptr);
        if (text_format_parse_digits(&cp, end_ptr, &n)) {
            // Positional width argument: *n$
            if (*cp != '$')
                ereport(ERROR, "width argument position must end with $");
            *widthpos = n;
            if (n == 0)
                ereport(ERROR, "argument positions start from 1, not 0");
            ADVANCE_PARSE_POINTER(cp, end_ptr);
        } else {
            // Non-positional width: just *
            *widthpos = 0;  // Use next argument for width
        }
    } else {
        // Direct width specification
        if (text_format_parse_digits(&cp, end_ptr, &n))
            *width = n;
    }

    return cp;  // Points to type specifier character
}
```