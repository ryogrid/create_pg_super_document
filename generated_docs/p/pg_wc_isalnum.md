# pg_wc_isalnum

## Location
[src/backend/regex/regc_pg_locale.c:362-395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L362-L395)

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
  - [pg_u_isalnum](pg_u_isalnum.md) (built-in Unicode alphanumeric test)
  - iswalnum (system wide character alphanumeric test)
  - isalnum (standard single-byte alphanumeric test)
  - iswalnum_l (locale-specific wide character alphanumeric test)
  - isalnum_l (locale-specific single-byte alphanumeric test)
  - u_isalnum (ICU alphanumeric classification)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (src/backend/regex/regc_locale.c:600)
  - [cclass_column_index](../c/cclass_column_index.md) (src/backend/regex/regc_locale.c:684)
  - [pg_wc_isword](pg_wc_isword.md) (src/backend/regex/regc_pg_locale.c:401)
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

## Simplified Source

```c
static int
pg_wc_isalnum(pg_wchar c)
{
    switch (pg_regex_strategy) {
        case PG_REGEX_LOCALE_C:
            // ASCII-only check using character properties table
            return (c <= 127 && (pg_char_properties[c] & PG_ISALNUM));

        case PG_REGEX_BUILTIN:
            // Use PostgreSQL's built-in Unicode classification
            return pg_u_isalnum(c, true);

        case PG_REGEX_LOCALE_WIDE:
            // Use system wide character function if supported
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswalnum((wint_t) c);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE:
            // Use standard single-byte function
            return (c <= UCHAR_MAX && isalnum((unsigned char) c));

        case PG_REGEX_LOCALE_WIDE_L:
            // Use locale-specific wide character function
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswalnum_l((wint_t) c, pg_regex_locale->info.lt);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE_L:
            // Use locale-specific single-byte function
            return (c <= UCHAR_MAX &&
                    isalnum_l((unsigned char) c, pg_regex_locale->info.lt));

        case PG_REGEX_LOCALE_ICU:
            // Use ICU library if available
            return u_isalnum(c);
    }

    return 0;  // Should never reach here
}
```