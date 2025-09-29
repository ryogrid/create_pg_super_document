# pg_wc_islower

## Location
[src/backend/regex/regc_pg_locale.c:439-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L439-L472)

## Overview
Determines whether a given wide character is a lowercase letter, handling multiple locale and encoding strategies for PostgreSQL's regex engine.

## Definition
```c
static int pg_wc_islower(pg_wchar c)
```

## Detailed Description
This function provides a unified interface for checking if a wide character is lowercase across different locale and encoding strategies used by PostgreSQL's regex subsystem. It implements the same strategy pattern as pg_wc_isupper, dispatching to appropriate locale-specific lowercase checking functions based on the current `pg_regex_strategy` setting.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses PostgreSQL's built-in character property table for ASCII characters
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's internal Unicode implementation
- **PG_REGEX_LOCALE_WIDE**: Uses system wide character functions for multi-byte locales
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte locale functions
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character functions
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte functions
- **PG_REGEX_LOCALE_ICU**: Uses ICU library functions when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for lowercase property

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property table)
  - [pg_u_islower](pg_u_islower.md) (PostgreSQL Unicode implementation)
  - iswlower (system wide character function)
  - islower (standard C library function)
  - iswlower_l (locale-specific wide character function)
  - islower_l (locale-specific single-byte function)
  - u_islower (ICU library function)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (regex character class processing)
  - [cclass_column_index](../c/cclass_column_index.md) (character class indexing)
  - REPLACEARC (regex arc replacement macro)

## Notes and Other Information
- Returns non-zero if the character is lowercase, 0 otherwise
- Part of PostgreSQL's regex character classification system
- Handles both single-byte and multi-byte character encodings
- Falls through from wide character strategies to single-byte strategies when characters exceed the wide character range
- ICU support is conditional on compile-time configuration (USE_ICU)
- Strategy selection is determined by `pg_regex_strategy` global variable
- Functionally parallel to pg_wc_isupper but for lowercase detection

## Simplified Source

```c
static int
pg_wc_islower(pg_wchar c)
{
    switch (pg_regex_strategy) {
        case PG_REGEX_LOCALE_C:
            // ASCII-only check using character properties table
            return (c <= 127 && (pg_char_properties[c] & PG_ISLOWER));

        case PG_REGEX_BUILTIN:
            // Use PostgreSQL's built-in Unicode classification
            return pg_u_islower(c);

        case PG_REGEX_LOCALE_WIDE:
            // Use system wide character function if supported
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswlower((wint_t) c);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE:
            // Use standard single-byte function
            return (c <= UCHAR_MAX && islower((unsigned char) c));

        case PG_REGEX_LOCALE_WIDE_L:
            // Use locale-specific wide character function
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswlower_l((wint_t) c, pg_regex_locale->info.lt);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE_L:
            // Use locale-specific single-byte function
            return (c <= UCHAR_MAX &&
                    islower_l((unsigned char) c, pg_regex_locale->info.lt));

        case PG_REGEX_LOCALE_ICU:
            // Use ICU library if available
            return u_islower(c);
    }

    return 0;  // Should never reach here
}
```