# pg_wc_isspace

## Location
[src/backend/regex/regc_pg_locale.c:575-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L575-L608)

## Overview
Determines whether a wide character is a whitespace character according to the current regex locale strategy.

## Definition

```c
static int
pg_wc_isspace(pg_wchar c)
```
## Detailed Description
This function checks if a given wide character (pg_wchar) is a whitespace character. The determination method depends on the current regex strategy (pg_regex_strategy), which allows PostgreSQL to handle different locale and character encoding scenarios consistently. The function implements a switch statement that handles six different regex strategies, each using appropriate locale-specific or encoding-specific whitespace detection methods.

The function supports multiple strategies:
- C locale using PostgreSQL's internal character properties
- Built-in Unicode handling via pg_u_isspace
- Wide character locale support using iswspace
- Single-byte locale support using standard isspace
- Locale-specific variants using _l functions
- ICU Unicode support when available

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to test for whitespace property
## Dependencies
- Functions called/Symbols referenced:
  - [pg_u_isspace](pg_u_isspace.md)
  - iswspace_l
  - isspace_l
  - u_isspace (ICU)
- Constants referenced:
  - PG_REGEX_LOCALE_C
  - PG_REGEX_BUILTIN
  - PG_REGEX_LOCALE_WIDE
  - PG_REGEX_LOCALE_1BYTE
  - PG_REGEX_LOCALE_WIDE_L
  - PG_REGEX_LOCALE_1BYTE_L
  - PG_REGEX_LOCALE_ICU
  - PG_ISSPACE
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md)
  - [cclass_column_index](../c/cclass_column_index.md)
  - iscspace

## Notes and Other Information
- The function is static and only used within the regex subsystem
- Returns 0 for non-whitespace characters and non-zero for whitespace characters
- Handles character range limitations properly for different locale strategies
- Falls through from wide character to single-byte handling when characters exceed wchar_t capacity
- The function ensures consistent whitespace detection across different PostgreSQL installations regardless of system locale configuration

## Simplified Source

```c
static int
pg_wc_isspace(pg_wchar c)
{
    switch (pg_regex_strategy) {
        case PG_REGEX_LOCALE_C:
            // ASCII-only check using character properties table
            return (c <= 127 && (pg_char_properties[c] & PG_ISSPACE));

        case PG_REGEX_BUILTIN:
            // Use PostgreSQL's built-in Unicode classification
            return pg_u_isspace(c);

        case PG_REGEX_LOCALE_WIDE:
            // Use system wide character function if supported
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswspace((wint_t) c);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE:
            // Use standard single-byte function
            return (c <= UCHAR_MAX && isspace((unsigned char) c));

        case PG_REGEX_LOCALE_WIDE_L:
            // Use locale-specific wide character function
            if (sizeof(wchar_t) >= 4 || c <= 0xFFFF)
                return iswspace_l((wint_t) c, pg_regex_locale->info.lt);
            // Fall through to single-byte if character too large

        case PG_REGEX_LOCALE_1BYTE_L:
            // Use locale-specific single-byte function
            return (c <= UCHAR_MAX &&
                    isspace_l((unsigned char) c, pg_regex_locale->info.lt));

        case PG_REGEX_LOCALE_ICU:
            // Use ICU library if available
            return u_isspace(c);
    }

    return 0;  // Should never reach here
}
```