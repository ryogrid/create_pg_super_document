# outToken

## Location
src/backend/nodes/outfuncs.c: 149 - 189

## Overview
Converts an ordinary string (such as an identifier) into a form that will be decoded back to a plain token by PostgreSQL's read functions, handling special character escaping and null/empty string encoding.

## Definition
void outToken(StringInfo str, const char *s)

## Detailed Description
The outToken function is a core utility in PostgreSQL's node serialization system that prepares strings for safe output in the serialized node format. It handles three main scenarios:

1. **Null pointer handling**: NULL string pointers are encoded as '<>'
2. **Empty string handling**: Empty strings are encoded as '\"\"'
3. **Special character escaping**: Strings containing characters that have special meaning in the parser (read.c functions) receive protective backslashes

The function carefully analyzes the input string to identify characters or patterns that need escaping:
- Characters requiring backslash protection at the start: '<', '\"', digits, and '+'/'-' followed by digits or '.'
- Characters requiring backslash protection anywhere: space, newline, tab, parentheses '()', braces '{}', and backslash '\\'

This encoding ensures that when the serialized output is later parsed by PostgreSQL's reading functions (pg_strtok() and nodeRead()), the original string content is correctly reconstructed.

## Parameters / Member Variables
- `str`: StringInfo buffer where the escaped token will be appended
- `s`: Input string to be converted/escaped (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString (for appending literal strings like '<>' and '\"\"')
  - appendStringInfoChar (for appending individual characters including escape backslashes)
  - isdigit (standard C library function for digit checking)

- Called from (representative examples):
  - WRITE_STRING_FIELD (macro in outfuncs.c:90)
  - outChar (function in outfuncs.c:204)
  - _outBoolExpr (function in outfuncs.c:422)
  - _outString (function in outfuncs.c:679)
  - _outBitString (function in outfuncs.c:692)

## Notes and Other Information
- This function is part of PostgreSQL's serialization infrastructure, working in conjunction with the parsing functions in read.c
- The escaping rules are specifically designed to be compatible with PostgreSQL's token parsing logic
- The function modifies the StringInfo buffer in-place by appending the escaped content
- Special attention is paid to numeric patterns to avoid confusion with numeric literals during parsing
- The function is used extensively throughout the node output system for safely serializing string data