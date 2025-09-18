# pg_wc_isgraph

## Location
src/backend/regex/regc_pg_locale.c: 473 - 506

## Overview
Determines whether a given wide character is a graphic character (visible character excluding spaces), handling multiple locale and encoding strategies for PostgreSQL's regex engine.

## Definition
```c
static int pg_wc_isgraph(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for checking if a wide character is a graphic character across different locale and encoding strategies used by PostgreSQL's regex subsystem. A graphic character is defined as any printable character except space - essentially any visible character that has a graphical representation.

The function implements the same strategy pattern as other pg_wc_* functions, dispatching to appropriate locale-specific graphic character checking functions based on the current `pg_regex_strategy` setting.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses PostgreSQL's built-in character property table for ASCII characters
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's internal Unicode implementation
- **PG_REGEX_LOCALE_WIDE**: Uses system wide character functions for multi-byte locales
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte locale functions
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character functions
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte functions
- **PG_REGEX_LOCALE_ICU**: Uses ICU library functions when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for graphic character property

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property table)
  - [pg_u_isgraph](pg_u_isgraph.md) (PostgreSQL Unicode implementation)
  - iswgraph (system wide character function)
  - isgraph (standard C library function)
  - iswgraph_l (locale-specific wide character function)
  - isgraph_l (locale-specific single-byte function)
  - u_isgraph (ICU library function)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (regex character class processing)
  - [cclass_column_index](../c/cclass_column_index.md) (character class indexing)
  - REPLACEARC (regex arc replacement macro)

## Notes and Other Information
- Returns non-zero if the character is a graphic character, 0 otherwise
- Graphic characters are printable characters excluding whitespace characters
- Part of PostgreSQL's regex character classification system
- Handles both single-byte and multi-byte character encodings
- Falls through from wide character strategies to single-byte strategies when characters exceed the wide character range
- ICU support is conditional on compile-time configuration (USE_ICU)
- Strategy selection is determined by `pg_regex_strategy` global variable
- Commonly used in regex pattern matching for visible character classes