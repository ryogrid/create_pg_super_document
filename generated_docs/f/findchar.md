# findchar

## Location
[src/backend/tsearch/spell.c:229-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L229-L241)

## Overview
A multibyte-aware character search function that locates the first occurrence of a specified ASCII character in a string, used in PostgreSQL's dictionary file parsing.

## Definition

```c
static char *
findchar(char *str, int c)
```
## Detailed Description
The  function is a specialized string search utility that finds the first occurrence of a character in a string while properly handling multibyte character encodings. Unlike standard C library functions like , this function is specifically designed to work with PostgreSQL's text search locale handling and multibyte character support.

The function iterates through the input string character by character, using  to advance by the correct number of bytes for each character in the current encoding. For each character position, it uses the  macro to compare with the target character. This approach ensures that multibyte characters are not incorrectly matched against single-byte ASCII characters.

The function is primarily used during dictionary file parsing to locate delimiter characters (like '/') that separate words from their affix flags in Ispell/Hunspell dictionary format.

## Parameters / Member Variables
- : Pointer to the null-terminated string to search in
- : The ASCII character to search for (must be a plain ASCII character)

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq (macro for character comparison, defined in ts_locale.h)
  - [pg_mblen](../p/pg_mblen.md) (function for getting multibyte character length)
  - TOUCHAR (macro used internally by t_iseq)
- Called from:
  - [NIImportDictionary](../N/NIImportDictionary.md) (src/backend/tsearch/spell.c:539) - for parsing dictionary entries

## Notes and Other Information
- This is a static function, accessible only within the spell.c compilation unit
- Returns a pointer to the first occurrence of the character, or NULL if not found
- The second parameter must be a plain ASCII character (as required by t_iseq macro)
- Handles multibyte encodings correctly by advancing through the string using pg_mblen()
- Used specifically in dictionary file parsing where '/' separates words from affix flags
- More robust than standard strchr() for PostgreSQL's internationalization requirements
- Part of PostgreSQL's text search infrastructure for processing Ispell/Hunspell dictionaries