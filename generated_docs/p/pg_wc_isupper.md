# pg_wc_isupper

## Location
[src/backend/regex/regc_pg_locale.c:405-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L405-L438)

## Overview
Determines whether a given wide character is an uppercase letter, handling multiple locale and encoding strategies for PostgreSQL's regex engine.

## Definition

```c
static int
pg_wc_isupper(pg_wchar c)
```
## Detailed Description
This function provides a unified interface for checking if a wide character is uppercase across different locale and encoding strategies used by PostgreSQL's regex subsystem. It implements a strategy pattern, dispatching to appropriate locale-specific uppercase checking functions based on the current  setting.

The function handles six different regex strategies:
- **PG_REGEX_LOCALE_C**: Uses PostgreSQL's built-in character property table for ASCII characters
- **PG_REGEX_BUILTIN**: Uses PostgreSQL's internal Unicode implementation
- **PG_REGEX_LOCALE_WIDE**: Uses system wide character functions for multi-byte locales
- **PG_REGEX_LOCALE_1BYTE**: Uses standard single-byte locale functions
- **PG_REGEX_LOCALE_WIDE_L**: Uses locale-specific wide character functions
- **PG_REGEX_LOCALE_1BYTE_L**: Uses locale-specific single-byte functions
- **PG_REGEX_LOCALE_ICU**: Uses ICU library functions when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for uppercase property
## Dependencies
- Functions called/Symbols referenced:
  - pg_char_properties (character property table)
  - [pg_u_isupper](pg_u_isupper.md) (PostgreSQL Unicode implementation)
  - iswupper (system wide character function)
  - isupper (standard C library function)
  - iswupper_l (locale-specific wide character function)
  - isupper_l (locale-specific single-byte function)
  - u_isupper (ICU library function)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (regex character class processing)
  - [cclass_column_index](../c/cclass_column_index.md) (character class indexing)
  - REPLACEARC (regex arc replacement macro)

## Notes and Other Information
- Returns non-zero if the character is uppercase, 0 otherwise
- Part of PostgreSQL's regex character classification system
- Handles both single-byte and multi-byte character encodings
- Falls through from wide character strategies to single-byte strategies when characters exceed the wide character range
- ICU support is conditional on compile-time configuration (USE_ICU)
- Strategy selection is determined by pg_regex_strategy global variable

## Simplified Source

```c
static int
pg_wc_isupper(pg_wchar c)
{
    switch (pg_regex_strategy) {
        case PG_REGEX_LOCALE_C:
            // ASCII-only check using character properties table
            return (c <= 127 && (pg_char_properties[c] & PG_ISUPPER));

        case PG_REGEX_BUILTIN:
            // Use PostgreSQL's built-in Unicode classification
            return pg_u_isupper(c);

        case PG_REGEX_LOCALE_WIDE:
            // Use system wide character function if supported
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswupper((wint_t) c);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE:
            // Use standard single-byte function
            return (c <= UCHAR_MAX && isupper((unsigned char) c));

        case PG_REGEX_LOCALE_WIDE_L:
            // Use locale-specific wide character function
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswupper_l((wint_t) c, pg_regex_locale->info.lt);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE_L:
            // Use locale-specific single-byte function
            return (c <= UCHAR_MAX &&
                    isupper_l((unsigned char) c, pg_regex_locale->info.lt));

        case PG_REGEX_LOCALE_ICU:
            // Use ICU library if available
            return u_isupper(c);
    }

    return 0;  // Should never reach here
}
```