# p_isurlchar

## Location
src/backend/tsearch/wparser_def.c: 505 - 536

## Overview
A static function that determines whether the current character in a text parser is a valid URL character according to RFC 3986 standards.

## Definition


## Detailed Description
This function validates whether the character at the current position in the text parser is suitable for inclusion in a URL. It performs multiple checks:

1. **Character length validation**: Only single-byte (ASCII) characters are accepted
2. **Control character filtering**: Rejects characters <= 0x20 (space and control characters) and >= 0x7F (extended ASCII)
3. **RFC 3986 compliance**: Explicitly rejects characters that are disallowed by RFC 3986 for URLs

The function is designed as part of PostgreSQL's text search parser to identify valid URL components during text parsing operations.

## Parameters / Member Variables
- : Pointer to TParser structure containing the current parsing state, character position, and text being parsed

## Dependencies
- Functions called/Symbols referenced:
  - TParser (structure type)
  - _make_compiler_happy (at line 535)
- Called from (representative examples):
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1462)
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1472)

## Notes and Other Information
- Returns 1 if the character is a valid URL character, 0 otherwise
- The function strictly adheres to RFC 3986 URL character restrictions
- Specifically rejects: double quote, less-than, greater-than, backslash, caret, backtick, curly braces, and pipe characters
- Used within PostgreSQL's text search functionality to properly tokenize URLs in text content
- The function assumes the parser is positioned at a valid character location