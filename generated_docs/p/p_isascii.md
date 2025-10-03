# p_isascii

## Location
[src/backend/tsearch/wparser_def.c:493-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L493-L498)

## Overview
A function that determines whether the current character in the parser is a valid ASCII character.

## Definition

```c
static int
p_isascii(TParser *prs)
```
## Detailed Description
The  function checks whether the current character at the parser's position is a valid ASCII character (values 0-127). It performs this check by first verifying that the character length is exactly 1 byte (indicating a single-byte character rather than a multi-byte Unicode character), and then using the standard  macro to test if the character falls within the ASCII range.

This function is essential for distinguishing between ASCII and non-ASCII characters during text parsing, which is important for proper tokenization in PostgreSQL's full-text search functionality. The function ensures that only single-byte characters are considered and properly casts the character to unsigned char before testing to avoid sign extension issues.

## Parameters / Member Variables
- `*prs`: Pointer to a TParser structure containing the current parsing state, including position information and the string being parsed
## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
  - isascii() (standard C library macro for ASCII character testing)
- Called from (representative examples):
  - [p_isasclet](p_isasclet.md) (used as part of ASCII letter testing)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Returns 1 if the current character is ASCII, 0 otherwise
- The character length check (charlen == 1) ensures we're dealing with single-byte characters
- Uses proper casting to unsigned char to prevent sign extension problems
- Part of the character classification system for text search parsing
- Critical for distinguishing ASCII from Unicode characters in multilingual text processing
- Used as a building block for more specific character type tests like 