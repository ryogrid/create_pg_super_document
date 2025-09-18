# PQmblen

## Location
src/interfaces/libpq/fe-misc.c: 1231 - 1241

## Overview
PQmblen provides a multibyte character length calculation interface that uses the dynamically-linked libpq's encoding stance, regardless of application startup differences.

## Definition
```c
int PQmblen(const char *s, int encoding)
```

## Detailed Description
This function serves as a wrapper around pg_encoding_mblen() specifically designed for applications that need consistent multibyte character behavior based on libpq's dynamic encoding determination rather than static compilation settings. It calculates the byte length of a multibyte character at the beginning of the provided string according to the specified encoding. This function was moved from fe-print.c as part of code reorganization.

## Parameters / Member Variables
- `s`: Pointer to the string containing the multibyte character to measure
- `encoding`: The character encoding identifier to use for length calculation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md)
- Called from (representative examples):
  - MAX_PROMPT_SIZE (src/bin/psql/prompt.c:364)
  - [pg_wcswidth](../p/pg_wcswidth.md) (src/fe_utils/mbprint.c:186)
  - [pg_wcssize](../p/pg_wcssize.md) (src/fe_utils/mbprint.c:223)
  - [pg_wcsformat](../p/pg_wcsformat.md) (src/fe_utils/mbprint.c:304)
  - strlen_max_width (src/fe_utils/print.c:3767)
  - [appendStringLiteral](../a/appendStringLiteral.md) (src/fe_utils/string_utils.c:385)

## Notes and Other Information
- Returns the byte length of the multibyte character at the start of string s
- Ensures consistent encoding behavior across different executable startups
- Part of libpq's multibyte character handling utilities
- Originally located in fe-print.c before being moved to fe-misc.c
- Used extensively in frontend utilities for proper text formatting and display