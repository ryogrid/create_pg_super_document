# PQmblenBounded

## Location
src/interfaces/libpq/fe-misc.c: 1242 - 1251

## Overview
PQmblenBounded provides a safe multibyte character length calculation that respects string boundaries, using libpq's dynamic encoding determination for consistent behavior.

## Definition
```c
int PQmblenBounded(const char *s, int encoding)
```

## Detailed Description
This function combines the functionality of pg_encoding_mblen() with strnlen() to provide a boundary-safe way of calculating multibyte character lengths. It ensures that the character length calculation never exceeds the actual string length, preventing potential buffer overruns when dealing with truncated or malformed multibyte sequences. Like other PQ multibyte functions, it uses the dynamically-linked libpq's encoding stance for consistency across different application startups.

## Parameters / Member Variables
- `s`: Pointer to the string containing the multibyte character to measure
- `encoding`: The character encoding identifier to use for length calculation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md)
  - strnlen
- Called from (representative examples):
  - [skip_white_space](../s/skip_white_space.md) (src/bin/psql/common.c:1839, 1876)
  - [command_no_begin](../c/command_no_begin.md) (src/bin/psql/common.c:1911, 1942, 1976, 1992, 2003, 2020, 2043, 2058, 2077, 2097)
  - [strtokx](../s/strtokx.md) (src/bin/psql/stringutils.c:146)
  - [strip_quotes](../s/strip_quotes.md) (src/bin/psql/stringutils.c:265)
  - [quote_if_needed](../q/quote_if_needed.md) (src/bin/psql/stringutils.c:327)
  - [parse_identifier](../p/parse_identifier.md) (src/bin/psql/tab-complete.c:6054)
  - [patternToSQLRegex](../p/patternToSQLRegex.md) (src/fe_utils/string_utils.c:1357)

## Notes and Other Information
- Returns the actual byte length of the multibyte character, bounded by the string length
- Provides safety against buffer overruns with truncated multibyte sequences
- Extensively used in psql command parsing and string processing operations
- Essential for safe text processing in multibyte character environments
- Combines encoding-aware length calculation with boundary checking for robust string handling