# pg_wc_isprint

## Location
src/backend/regex/regc_pg_locale.c: 507 - 540

## Overview
Determines whether a given wide character is a printable character (including spaces), handling multiple locale and encoding strategies for PostgreSQL's regex engine.

## Definition
```c
static int pg_wc_isprint(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for checking if a wide character is printable across different locale and encoding strategies used by PostgreSQL's regex subsystem. A printable character is any character that has a visual representation when displayed, including spaces but excluding control characters.

The function implements the same strategy pattern as other pg_wc_* functions, dispatching to appropriate locale-specific printable character checking functions based on the current `pg_regex_strategy` setting.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses PostgreSQL's built-in character property table for ASCII characters
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's internal Unicode implementation
- **PG_REGEX_LOCALE_WIDE**: Uses system wide character functions for multi-byte locales
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte locale functions
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character functions
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte functions
- **PG_REGEX_LOCALE_ICU**: Uses ICU library functions when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for printable character property

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property table)
  - [pg_u_isprint](pg_u_isprint.md) (PostgreSQL Unicode implementation)
  - iswprint (system wide character function)
  - isprint (standard C library function)
  - iswprint_l (locale-specific wide character function)
  - isprint_l (locale-specific single-byte function)
  - u_isprint (ICU library function)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (regex character class processing)
  - [cclass_column_index](../c/cclass_column_index.md) (character class indexing)
  - REPLACEARC (regex arc replacement macro)

## Notes and Other Information
- Returns non-zero if the character is printable, 0 otherwise
- Printable characters include all graphic characters plus space characters
- Excludes control characters and non-displayable characters
- Part of PostgreSQL's regex character classification system
- Handles both single-byte and multi-byte character encodings
- Falls through from wide character strategies to single-byte strategies when characters exceed the wide character range
- ICU support is conditional on compile-time configuration (USE_ICU)
- Strategy selection is determined by `pg_regex_strategy` global variable
- Broader category than pg_wc_isgraph as it includes whitespace characters