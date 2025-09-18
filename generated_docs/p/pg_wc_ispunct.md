# pg_wc_ispunct

## Location
src/backend/regex/regc_pg_locale.c: 541 - 574

## Overview
Determines whether a given wide character is a punctuation character, handling multiple locale and encoding strategies for PostgreSQL's regex engine.

## Definition
```c
static int pg_wc_ispunct(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for checking if a wide character is a punctuation character across different locale and encoding strategies used by PostgreSQL's regex subsystem. Punctuation characters are printable characters that are not alphanumeric or spaces - typically symbols used for formatting and structure in text.

The function implements the same strategy pattern as other pg_wc_* functions, dispatching to appropriate locale-specific punctuation character checking functions based on the current `pg_regex_strategy` setting.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses PostgreSQL's built-in character property table for ASCII characters
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's internal Unicode implementation (with special parameter `true`)
- **PG_REGEX_LOCALE_WIDE**: Uses system wide character functions for multi-byte locales
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte locale functions
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character functions
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte functions
- **PG_REGEX_LOCALE_ICU**: Uses ICU library functions when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for punctuation character property

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property table)
  - [pg_u_ispunct](pg_u_ispunct.md) (PostgreSQL Unicode implementation, called with `true` parameter)
  - iswpunct (system wide character function)
  - ispunct (standard C library function)
  - iswpunct_l (locale-specific wide character function)
  - ispunct_l (locale-specific single-byte function)
  - u_ispunct (ICU library function)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (regex character class processing)
  - [cclass_column_index](../c/cclass_column_index.md) (character class indexing)
  - REPLACEARC (regex arc replacement macro)

## Notes and Other Information
- Returns non-zero if the character is a punctuation character, 0 otherwise
- Punctuation characters include symbols like !@#$%^&*(),.;:'"?/-= but exclude alphanumeric characters and spaces
- Part of PostgreSQL's regex character classification system
- Handles both single-byte and multi-byte character encodings
- Falls through from wide character strategies to single-byte strategies when characters exceed the wide character range
- ICU support is conditional on compile-time configuration (USE_ICU)
- Strategy selection is determined by `pg_regex_strategy` global variable
- Notable difference: pg_u_ispunct is called with a `true` parameter, unlike other pg_wc_* functions
- Commonly used in regex patterns for matching punctuation and symbol characters