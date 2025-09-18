# pg_wc_isalpha

## Location
src/backend/regex/regc_pg_locale.c: 328 - 361

## Overview
A static function that determines whether a wide character is an alphabetic character, using the appropriate locale-aware method based on the current regex strategy.

## Definition
```c
static int pg_wc_isalpha(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for alphabetic character classification that works across different locale strategies and character encodings. Similar to `pg_wc_isdigit`, it switches between different implementations based on the current `pg_regex_strategy` value established by `pg_set_regex_collation`.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses ASCII-only alphabetic classification with character properties array
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's built-in Unicode alphabetic classification
- **PG_REGEX_LOCALE_WIDE**: Uses system's wide character alphabetic classification (iswalpha)
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte alphabetic classification (isalpha)
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character alphabetic classification
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte alphabetic classification
- **PG_REGEX_LOCALE_ICU**: Uses ICU library's alphabetic classification when available

## Parameters / Member Variables
- `c`: A wide character (pg_wchar type) to test for alphabetic classification

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property lookup table)
  - pg_u_isalpha (built-in Unicode alphabetic test)
  - iswalpha (system wide character alphabetic test)
  - isalpha (standard single-byte alphabetic test)
  - iswalpha_l (locale-specific wide character alphabetic test)
  - isalpha_l (locale-specific single-byte alphabetic test)
  - u_isalpha (ICU alphabetic classification)
- Called from (representative examples):
  - cclasscvec (src/backend/regex/regc_locale.c:603)
  - cclass_column_index (src/backend/regex/regc_locale.c:686)
  - REPLACEARC (src/backend/regex/regcomp.c:253)
  - iscalpha (src/include/regex/regcustom.h:92)

## Notes and Other Information
- Static function - only accessible within the same source file
- Returns non-zero (true) if character is alphabetic, 0 (false) otherwise
- Handles character encoding limits appropriately (UCHAR_MAX for single-byte, 0xFFFF for wide characters)
- Falls through from wide character cases to single-byte cases when character is out of wide character range
- Relies on global `pg_regex_strategy` and `pg_regex_locale` variables set by `pg_set_regex_collation`
- Follows the same pattern as other character classification functions in the regex subsystem
- Location: src/backend/regex/regc_pg_locale.c:328-361