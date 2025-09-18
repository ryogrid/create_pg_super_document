# pg_strcoll

## Location
[src/backend/utils/adt/pg_locale.c:2121-2155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2121-L2155)

## Overview
Main public interface for locale-aware string collation that automatically dispatches to the appropriate collation provider (libc or ICU) based on locale configuration.

## Definition
```c
int pg_strcoll(const char *arg1, const char *arg2, pg_locale_t locale)
```

## Detailed Description
The `pg_strcoll` function serves as PostgreSQL's primary interface for locale-aware string collation. It acts as a dispatcher that automatically selects the appropriate underlying collation implementation based on the locale's provider type. For libc-based locales (or when no locale is specified), it delegates to `pg_strcoll_libc`. For ICU-based locales, it calls `pg_strncoll_icu` with -1 length indicators to signal null-terminated strings. The function is designed to maintain API consistency across different collation providers while leveraging the specific strengths of each implementation.

## Parameters / Member Variables
- `arg1`: First null-terminated string to compare, encoded in database encoding
- `arg2`: Second null-terminated string to compare, encoded in database encoding  
- `locale`: PostgreSQL locale object specifying collation rules, or NULL for database default collation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcoll_libc](pg_strcoll_libc.md)
  - [pg_strncoll_icu](pg_strncoll_icu.md)
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - [varstrfastcmp_locale](../v/varstrfastcmp_locale.md)

## Notes and Other Information
- This is a public function (non-static) serving as the main collation API
- Handles both libc and ICU collation providers transparently
- Includes error handling for unsupported locale provider types
- Expects null-terminated strings (uses -1 length for ICU calls to indicate this)
- The caller is responsible for breaking ties in deterministic collations for consistency with pg_strxfrm()
- Conditionally compiled ICU support based on USE_ICU preprocessing directive
- Part of PostgreSQL's unified locale abstraction layer