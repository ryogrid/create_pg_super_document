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
- : The wide character (pg_wchar) to test for whitespace property

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