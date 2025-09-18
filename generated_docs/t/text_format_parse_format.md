# text_format_parse_format

## Location
src/backend/utils/adt/varlena.c: 5964 - 6040

## Overview
The  function parses the components of a printf-style format specifier, extracting argument positions, flags, width specifications, and other formatting parameters.

## Definition


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
- : Pointer to the character after the initial '%' in the format specifier (input)
- : Pointer to the end of the format string (input, boundary check)
- : Parsed argument position for the value to be formatted, -1 if unspecified (output)
- : Argument position for width value, 0 if next argument should be used, -1 if no width argument (output)
- : Bitmask of formatting flags (currently only supports ) (output)
- : Direct width specification, 0 if width was omitted (output)

## Dependencies
- Functions called/Symbols referenced:
  -  - Parse numeric values from format string (called multiple times)
  -  - Macro for safe pointer advancement with bounds checking
  -  - Flag constant for left-alignment formatting
  - , , ,  - PostgreSQL error reporting system
- Called from (representative examples):
  -  - Main FORMAT() function implementation
  -  - Variable-length string datum extraction

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