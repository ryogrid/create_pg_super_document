# t_isprint

## Location
[src/backend/tsearch/ts_locale.c:95-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L95-L133)

## Overview
The t_isprint function checks whether a character is printable, with proper support for multi-byte characters and locale-aware printable character classification in text search contexts.

## Definition
```c
int t_isprint(const char *ptr)
```

## Detailed Description
This function provides locale-aware printable character detection for PostgreSQL's text search functionality. It handles both single-byte and multi-byte character encodings. For single-byte characters or when the database uses C locale, it uses the standard isprint() function. For multi-byte characters, it converts the character to wide character format and uses iswprint() for proper Unicode printable character classification.

A printable character is any character that produces visible output, including letters, digits, punctuation, and symbols, but excluding control characters. This function is used primarily in dictionary import operations where ensuring character validity is important.

## Parameters / Member Variables
- `ptr`: Pointer to the character string to test for printable classification

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mblen](../p/pg_mblen.md)
  - [char2wchar](../c/char2wchar.md)
  - isprint (standard C library)
  - iswprint (standard C library)
  - TOUCHAR
- Called from (representative examples):
  - [NIImportDictionary](../N/NIImportDictionary.md) (dictionary import)
  - COPYCHAR

## Notes and Other Information
- Uses pg_locale_t mylocale = 0 with a TODO comment, indicating incomplete locale support implementation
- Falls back to single-byte character handling when database_ctype_is_c is true
- Part of PostgreSQL's text search locale abstraction layer defined in ts_locale.h
- Least frequently used among the character classification functions in the text search system
- Primarily used for data validation during dictionary import operations
- Helps ensure that only valid, printable characters are processed in text search operations

## Simplified Source

```c
int t_isprint(const char *ptr) {
    int clen = pg_mblen(ptr);

    // For single-byte chars or C locale, use standard isprint
    if (clen == 1 || database_ctype_is_c)
        return isprint(TOUCHAR(ptr));

    // For multi-byte chars, convert to wide char and check
    wchar_t character[WC_BUF_LEN];
    char2wchar(character, WC_BUF_LEN, ptr, clen, 0);
    return iswprint((wint_t) character[0]);
}
```