# PG_Locale_Strategy

## Location
src/backend/regex/regc_pg_locale.c: 75 - 83

## Overview
An enumeration that defines different strategies for locale-dependent character type operations in PostgreSQL's regular expression engine.

## Definition


## Detailed Description
The PG_Locale_Strategy enum is used internally by PostgreSQL's regular expression engine to determine which character classification and case conversion functions to use based on the current locale and encoding. This enum enables the regex engine to adapt its character handling behavior across different platforms and locale configurations while maintaining consistent behavior.

The strategy selection depends on several factors:
- The active collation (C/POSIX vs. locale-specific)
- The database encoding (UTF-8 vs. single-byte encodings)
- The availability of locale_t functions on the platform
- ICU library availability

The chosen strategy affects how character classification functions (isalpha, isdigit, etc.) and case conversion functions (toupper, tolower) operate on wide characters (pg_wchar).

## Parameters / Member Variables
- : Uses hard-wired character properties for C locale, independent of database encoding
- : Uses PostgreSQL's built-in Unicode character classification functions
- : Uses standard <wctype.h> functions for Unicode/wide character handling
- : Uses standard <ctype.h> functions for single-byte character handling
- : Uses locale_t-based <wctype.h> functions for thread-safe locale-specific operations
- : Uses locale_t-based <ctype.h> functions for thread-safe single-byte operations
- : Uses ICU library's uchar.h functions for comprehensive Unicode support

## Dependencies
- Functions that use this strategy:
  - [pg_set_regex_collation](../p/pg_set_regex_collation.md) (sets the strategy in src/backend/regex/regc_pg_locale.c:234)
  - [pg_wc_isdigit](../p/pg_wc_isdigit.md), pg_wc_isalpha, pg_wc_isalnum, pg_wc_isupper, pg_wc_islower
  - [pg_wc_isgraph](../p/pg_wc_isgraph.md), pg_wc_isprint, pg_wc_ispunct, pg_wc_isspace
  - pg_wc_toupper, pg_wc_tolower
- Static variable:
  - pg_regex_strategy (stores the current strategy)

## Notes and Other Information
- The strategy is stored in a static variable and set by pg_set_regex_collation() at the beginning of regex compilation or execution
- Strategy selection prioritizes ICU when available, falls back to built-in Unicode functions for UTF-8, and uses platform ctype functions for other encodings
- ASCII characters (0-127) are handled with forced C behavior in some strategies to ensure consistent behavior across locales
- The choice of strategy directly impacts the behavior of character classes in regular expressions (e.g., \w, \d, [:alpha:])
- This design allows PostgreSQL to provide maximum functionality across diverse platforms while maintaining predictable regex behavior