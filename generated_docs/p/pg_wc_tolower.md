# pg_wc_tolower

## Location
[src/backend/regex/regc_pg_locale.c:651-707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L651-L707)

## Overview
Converts a wide character to its lowercase equivalent according to the current regex locale strategy.

## Definition
```c
static pg_wchar pg_wc_tolower(pg_wchar c)
```

## Detailed Description
This function converts a given wide character (pg_wchar) to its lowercase form. It follows the same pattern as pg_wc_toupper, using the current regex strategy (pg_regex_strategy) to determine the appropriate conversion method. The function implements a comprehensive switch statement that handles six different regex strategies, each using appropriate locale-specific or encoding-specific lowercase conversion methods.

Like its uppercase counterpart, for ASCII characters (0-127), the function forces C locale behavior in most strategies to ensure consistent results regardless of the system locale. For non-ASCII characters, it delegates to the appropriate system or Unicode conversion function based on the active strategy.

## Parameters / Member Variables
- `c`: The wide character (pg_wchar) to convert to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - pg_ascii_tolower
  - unicode_lowercase_simple
  - towlower
  - tolower
  - towlower_l
  - tolower_l
  - u_tolower (ICU)
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
  - casecmp
  - REPLACEARC

## Notes and Other Information
- The function is static and only used within the regex subsystem
- Returns the lowercase equivalent of the input character, or the original character if no lowercase form exists
- Forces C locale behavior for ASCII characters in wide and single-byte locale strategies to ensure portability
- Handles character range limitations properly, falling through from wide character to single-byte handling when needed
- For characters outside the supported range of the current strategy, returns the original character unchanged
- The function ensures predictable case conversion behavior across different PostgreSQL installations
- Used more frequently than pg_wc_toupper, including in case-insensitive comparison operations