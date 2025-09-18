# pg_wc_isalnum

## Location
src/backend/regex/regc_pg_locale.c: 362 - 395

## Overview
A static function that determines whether a wide character is alphanumeric (alphabetic or digit), using the appropriate locale-aware method based on the current regex strategy.

## Definition
```c
static int pg_wc_isalnum(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for alphanumeric character classification that works across different locale strategies and character encodings. It combines both alphabetic and digit character classification, switching between different implementations based on the current `pg_regex_strategy` value established by `pg_set_regex_collation`.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses ASCII-only alphanumeric classification with character properties array
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's built-in Unicode alphanumeric classification
- **PG_REGEX_LOCALE_WIDE**: Uses system's wide character alphanumeric classification (iswalnum)
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte alphanumeric classification (isalnum)
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character alphanumeric classification
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte alphanumeric classification
- **PG_REGEX_LOCALE_ICU**: Uses ICU library's alphanumeric classification when available

## Parameters / Member Variables
- `c`: A wide character (pg_wchar type) to test for alphanumeric classification

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property lookup table)
  - pg_u_isalnum (built-in Unicode alphanumeric test)
  - iswalnum (system wide character alphanumeric test)
  - isalnum (standard single-byte alphanumeric test)
  - iswalnum_l (locale-specific wide character alphanumeric test)
  - isalnum_l (locale-specific single-byte alphanumeric test)
  - u_isalnum (ICU alphanumeric classification)
- Called from (representative examples):
  - cclasscvec (src/backend/regex/regc_locale.c:600)
  - cclass_column_index (src/backend/regex/regc_locale.c:684)
  - pg_wc_isword (src/backend/regex/regc_pg_locale.c:401)
  - REPLACEARC (src/backend/regex/regcomp.c:254)
  - iscalnum (src/include/regex/regcustom.h:91)

## Notes and Other Information
- Static function - only accessible within the same source file
- Returns non-zero (true) if character is alphanumeric, 0 (false) otherwise
- Handles character encoding limits appropriately (UCHAR_MAX for single-byte, 0xFFFF for wide characters)
- Falls through from wide character cases to single-byte cases when character is out of wide character range
- Relies on global `pg_regex_strategy` and `pg_regex_locale` variables set by `pg_set_regex_collation`
- Used as a component in `pg_wc_isword` function for word character classification
- Location: src/backend/regex/regc_pg_locale.c:362-395