# str_udeescape

## Location
src/backend/parser/parser.c: 372 - 527

## Overview
Processes Unicode escapes in SQL string literals, converting escape sequences like \XXXX and \+XXXXXX to their corresponding Unicode characters in the server encoding.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's lexical analyzer that handles Unicode escape sequences in SQL string literals marked with U&'' or U&"" syntax. It parses two types of Unicode escape sequences:

1. **4-digit format**:  where XXXX is a 4-digit hexadecimal Unicode code point
2. **6-digit format**:  where XXXXXX is a 6-digit hexadecimal Unicode code point

The function properly handles UTF-16 surrogate pairs for Unicode code points above U+FFFF, validates Unicode values, and converts them to the server's character encoding. It dynamically allocates memory for the output string and handles escape character doubling (where the escape character followed by itself represents a literal escape character).

## Parameters / Member Variables
- : Input string containing Unicode escape sequences to be processed
- : The escape character used in the string (typically backslash)
- : Starting position of the U&'' or U&"" string token for error reporting
- : Scanner context information needed for generating accurate error reports and positioning

## Dependencies
- Functions called/Symbols referenced:
  - [hexval](../h/hexval.md) (converts hexadecimal character to numeric value)
  - [check_unicode_value](../c/check_unicode_value.md) (validates Unicode code point)
  - is_utf16_surrogate_first (checks if code point is first half of surrogate pair)
  - is_utf16_surrogate_second (checks if code point is second half of surrogate pair)
  - surrogate_pair_to_codepoint (combines surrogate pair into full Unicode code point)
  - [pg_unicode_to_server](../p/pg_unicode_to_server.md) (converts Unicode to server encoding)
  - setup_scanner_errposition_callback (sets up error positioning)
  - cancel_scanner_errposition_callback (cleans up error positioning)
  - [repalloc](../r/repalloc.md) (reallocates memory for growing output buffer)
- Called from (representative examples):
  - [base_yylex](../b/base_yylex.md) (PostgreSQL lexical analyzer at lines 286 and 302)

## Notes and Other Information
- The function uses dynamic memory allocation with initial size estimation and buffer expansion as needed
- Proper error handling includes detailed syntax error messages with cursor positioning
- UTF-16 surrogate pair validation ensures only valid Unicode sequences are processed
- The MAX_UNICODE_EQUIVALENT_STRING constant provides padding for Unicode-to-server encoding conversion
- Memory management uses PostgreSQL's palloc/repalloc functions for automatic cleanup on error
- Function is static and only used within the parser module for lexical analysis