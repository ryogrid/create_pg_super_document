# pg_wc_isdigit

## Location
[src/backend/regex/regc_pg_locale.c:294-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L294-L327)

## Overview
A static function that determines whether a wide character is a digit, using the appropriate locale-aware method based on the current regex strategy.

## Definition
```c
static int pg_wc_isdigit(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for digit character classification that works across different locale strategies and character encodings. It switches between different implementations based on the current `pg_regex_strategy` value, which is set by `pg_set_regex_collation`.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses ASCII-only digit classification with character properties array
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's built-in Unicode digit classification
- **PG_REGEX_LOCALE_WIDE**: Uses system's wide character digit classification (iswdigit)
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte digit classification (isdigit)
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character digit classification
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte digit classification
- **PG_REGEX_LOCALE_ICU**: Uses ICU library's digit classification when available

## Parameters / Member Variables
- `c`: A wide character (pg_wchar type) to test for digit classification

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property lookup table)
  - [pg_u_isdigit](pg_u_isdigit.md) (built-in Unicode digit test)
  - iswdigit (system wide character digit test)
  - isdigit (standard single-byte digit test)
  - iswdigit_l (locale-specific wide character digit test)
  - isdigit_l (locale-specific single-byte digit test)
  - u_isdigit (ICU digit classification)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (src/backend/regex/regc_locale.c:627)
  - [cclass_column_index](../c/cclass_column_index.md) (src/backend/regex/regc_locale.c:693)
  - REPLACEARC (src/backend/regex/regcomp.c:252)
  - iscdigit (src/include/regex/regcustom.h:93)

## Notes and Other Information
- Static function - only accessible within the same source file
- Returns non-zero (true) if character is a digit, 0 (false) otherwise
- Handles character encoding limits appropriately (UCHAR_MAX for single-byte, 0xFFFF for wide characters)
- Falls through from wide character cases to single-byte cases when character is out of wide character range
- Relies on global `pg_regex_strategy` and `pg_regex_locale` variables set by `pg_set_regex_collation`
- Location: src/backend/regex/regc_pg_locale.c:294-327