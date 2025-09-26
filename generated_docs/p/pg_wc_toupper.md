# pg_wc_toupper

## Location
[src/backend/regex/regc_pg_locale.c:609-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L609-L650)

## Overview
Converts a wide character to its uppercase equivalent according to the current regex locale strategy.

## Definition
```c
static pg_wchar pg_wc_toupper(pg_wchar c)
```

## Detailed Description
This function converts a given wide character (pg_wchar) to its uppercase form. Like pg_wc_isspace, the conversion method depends on the current regex strategy (pg_regex_strategy), ensuring consistent case conversion across different locale and character encoding scenarios. The function implements a comprehensive switch statement that handles six different regex strategies, each using appropriate locale-specific or encoding-specific uppercase conversion methods.

For ASCII characters (0-127), the function forces C locale behavior in most strategies to ensure consistent results regardless of the system locale. For non-ASCII characters, it delegates to the appropriate system or Unicode conversion function based on the active strategy.

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to convert to uppercase

## Dependencies
- Functions called/Symbols referenced:
  - pg_ascii_toupper
  - unicode_uppercase_simple
  - towupper
  - toupper
  - towupper_l
  - toupper_l
  - u_toupper (ICU)
- Constants referenced:
  - PG_REGEX_LOCALE_C
  - PG_REGEX_BUILTIN
  - PG_REGEX_LOCALE_WIDE
  - PG_REGEX_LOCALE_1BYTE
  - PG_REGEX_LOCALE_WIDE_L
  - PG_REGEX_LOCALE_1BYTE_L
  - PG_REGEX_LOCALE_ICU
- Called from (representative examples):
  - range
  - allcases
  - REPLACEARC

## Notes and Other Information
- The function is static and only used within the regex subsystem
- Returns the uppercase equivalent of the input character, or the original character if no uppercase form exists
- Forces C locale behavior for ASCII characters in wide and single-byte locale strategies to ensure portability
- Handles character range limitations properly, falling through from wide character to single-byte handling when needed
- For characters outside the supported range of the current strategy, returns the original character unchanged
- The function ensures predictable case conversion behavior across different PostgreSQL installations