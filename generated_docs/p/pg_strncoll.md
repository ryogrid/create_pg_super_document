# pg_strncoll

## Location
[src/backend/utils/adt/pg_locale.c:2156-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2156-L2175)

## Overview
This function performs locale-aware string comparison for two strings with specified lengths, supporting both libc and ICU collation providers based on the locale configuration.

## Definition

```c
int
pg_strncoll(const char *arg1, size_t len1, const char *arg2, size_t len2,
			pg_locale_t locale)
```
## Detailed Description
pg_strncoll is a dispatch function that performs collation-aware string comparison by delegating to the appropriate underlying implementation based on the locale provider. It supports both traditional libc-based collations and modern ICU-based collations. The function handles strings that may not be null-terminated by accepting explicit length parameters, making it suitable for use with PostgreSQL's internal string representations.

The function operates as a wrapper that:
- Determines the collation provider from the locale parameter
- Dispatches to pg_strncoll_libc() for libc-based collations
- Dispatches to pg_strncoll_icu() for ICU-based collations (when ICU support is compiled in)
- Returns an integer indicating the comparison result following standard C library conventions

Input strings must be encoded in the database encoding. The caller is responsible for handling deterministic collation tie-breaking to maintain consistency with transformation functions.

## Parameters / Member Variables
- : First string to compare (database encoded)
- : Length of the first string in bytes
- : Second string to compare (database encoded)  
- : Length of the second string in bytes
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: Locale specification determining collation rules and provider

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strncoll_libc](pg_strncoll_libc.md)
  - [pg_strncoll_icu](pg_strncoll_icu.md)
  - COLLPROVIDER_LIBC
  - COLLPROVIDER_ICU
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - [varstr_cmp](../v/varstr_cmp.md) (src/backend/utils/adt/varlena.c:1575)

## Notes and Other Information
- This function may need to null-terminate arguments for libc functions internally
- If the caller already has null-terminated strings, pg_strcoll() should be used instead for efficiency
- The caller must handle deterministic collation tie-breaking for consistency with pg_strnxfrm()
- Returns standard C library comparison result: negative for less than, zero for equal, positive for greater than
- Located in src/backend/utils/adt/pg_locale.c:2156-2175