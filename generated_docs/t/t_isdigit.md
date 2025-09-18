# t_isdigit

## Location
src/backend/tsearch/ts_locale.c: 35 - 49

## Overview
The t_isdigit function checks whether a character is a digit, with proper support for multi-byte characters and locale-aware digit classification in text search contexts.

## Definition


## Detailed Description
This function provides locale-aware digit detection for PostgreSQL's text search functionality. It handles both single-byte and multi-byte character encodings. For single-byte characters or when the database uses C locale, it uses the standard isdigit() function. For multi-byte characters, it converts the character to wide character format and uses iswdigit() for proper Unicode digit classification.

The function is part of PostgreSQL's text search locale handling infrastructure, ensuring that digit classification works correctly across different character encodings and locales.

## Parameters / Member Variables
- : Pointer to the character string to test for digit classification

## Dependencies
- Functions called/Symbols referenced:
  - pg_mblen
  - char2wchar
  - isdigit (standard C library)
  - iswdigit (standard C library)
  - TOUCHAR
- Called from (representative examples):
  - getNextFlagFromString
  - NISortDictionary
  - PHRASE_FINISH
  - gettoken_tsvector
  - COPYCHAR

## Notes and Other Information
- Uses pg_locale_t mylocale = 0 with a TODO comment, indicating incomplete locale support implementation
- Falls back to single-byte character handling when database_ctype_is_c is true
- Part of PostgreSQL's text search locale abstraction layer defined in ts_locale.h
- WC_BUF_LEN constant is used for wide character buffer sizing