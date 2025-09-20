# pg_strxfrm_prefix

## Location
[src/backend/utils/adt/pg_locale.c:2490-2524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2490-L2524)

## Overview
Transforms a string to a byte sequence that can be compared using memcmp() to achieve the same ordering as pg_strcoll() on the original strings.

## Definition

```c
size_t
pg_strxfrm_prefix(char *dest, const char *src, size_t destsize,
				  pg_locale_t locale)
```
## Detailed Description
The pg_strxfrm_prefix function performs string transformation for collation purposes. It converts the input string 'src' into a binary representation stored in 'dest' such that lexicographic comparison (memcmp) of the transformed strings yields the same ordering as locale-aware string comparison (pg_strcoll) of the original strings. This transformation is essential for efficient sorting and indexing operations where locale-specific collation rules must be preserved.

The function supports different collation providers through the locale parameter. Currently, it primarily handles ICU-based collations by delegating to pg_strnxfrm_prefix_icu for the actual transformation work. For unsupported providers, it raises appropriate errors.

The result is not null-terminated, and if the destination buffer is too small, only the first destsize bytes are stored.

## Parameters / Member Variables
- : Output buffer to store the transformed byte sequence
- : Input null-terminated string to be transformed
- : Maximum number of bytes to store in the destination buffer
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
LC_ALL=: Locale information specifying the collation provider and rules to use

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strnxfrm_prefix_icu](pg_strnxfrm_prefix_icu.md)
  - PGLOCALE_SUPPORT_ERROR
  - COLLPROVIDER_ICU
  - COLLPROVIDER_LIBC
  - pg_locale_t
- Called from (representative examples):
  - [varstr_abbrev_convert](../v/varstr_abbrev_convert.md)

## Notes and Other Information
- The function currently only supports ICU collation provider; LIBC provider results in an error
- Returns the actual number of bytes copied to the destination buffer
- This is part of PostgreSQL's locale and collation infrastructure
- The transformation enables efficient comparison operations while preserving locale-specific sorting rules
- Used primarily in query optimization for abbreviated key comparisons