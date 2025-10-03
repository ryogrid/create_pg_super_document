# pg_mblen

## Location
[src/backend/utils/mb/mbutils.c:1023-1029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1023-L1029)

## Overview
Returns the byte length of a multibyte character at the beginning of a string.

## Definition

```c
int
pg_mblen(const char *mbstr)
```
## Detailed Description
 is a utility function that determines the byte length of the first multibyte character in a given string. It serves as a wrapper around the encoding-specific multibyte length function stored in the  for the current database encoding. This function is essential for proper multibyte character handling in PostgreSQL, allowing code to advance through multibyte strings character by character rather than byte by byte.

The function delegates to the appropriate encoding-specific implementation through the function pointer , which ensures that the correct multibyte handling logic is applied based on the database's character encoding (UTF-8, EUC-JP, etc.).

## Parameters / Member Variables
- `*mbstr`: Pointer to the start of a multibyte string. The function examines the character at this position to determine its byte length.
## Dependencies
- Functions called/Symbols referenced:
  -  (global encoding function table)
  -  (current database encoding information)
- Called from (representative examples):
  -  (multibyte string length calculation)
  -  (multibyte string length with boundary check)
  -  (text substring operations)
  -  (character translation functions)
  - Various text search and formatting functions

## Notes and Other Information
- The function assumes the input string is valid and contains at least one character
- Returns the number of bytes (1 or more) that comprise the first character
- Essential for implementing multibyte-aware string operations in PostgreSQL
- Used extensively throughout the codebase for text processing, formatting, and search operations
- Part of PostgreSQL's multibyte character support infrastructure

## Simplified Source

```c
int pg_mblen(const char *mbstr) {
    // Get byte length of first multibyte character using encoding-specific function
    return pg_wchar_table[DatabaseEncoding->encoding].mblen((const unsigned char *) mbstr);
}
```