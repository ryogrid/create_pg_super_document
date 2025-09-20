# mb_strchr

## Location
[src/backend/tsearch/regis.c:182-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L182-L212)

## Overview
Searches for a multibyte character within a string, similar to strchr but with proper multibyte character support for PostgreSQL's text search system.

## Definition

```c
static bool
mb_strchr(char *str, char *c)
```
## Detailed Description
mb_strchr provides multibyte-aware character searching functionality, acting as a replacement for the standard strchr function when dealing with multibyte character encodings. The function iterates through the input string character by character, using pg_mblen to determine the length of each multibyte character. For each character in the string, it compares both the length and the byte sequence with the target character. If a match is found (same length and identical byte sequence), it returns true; otherwise, it continues searching until the end of the string.

## Parameters / Member Variables
- : The string to search within
- : The multibyte character to search for

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mblen](../p/pg_mblen.md) (get multibyte character length)
- Called from:
  - [RS_execute](../R/RS_execute.md) (at lines 242 and 246)

## Notes and Other Information
- Static function, only accessible within regis.c
- Returns boolean (true if character found, false otherwise)
- Handles variable-length multibyte characters correctly
- Essential for international text support in PostgreSQL's text search
- Performs byte-by-byte comparison for matching characters of the same length
- Part of the regex execution infrastructure for pattern matching
- More robust than standard strchr for non-ASCII character sets