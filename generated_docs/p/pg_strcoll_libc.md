# pg_strcoll_libc

## Location
[src/backend/utils/adt/pg_locale.c:1939-1967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1939-L1967)

## Overview
Performs locale-aware string collation using the system's libc implementation, handling platform-specific variations and encoding requirements.

## Definition


## Detailed Description
The  function provides a unified interface for string collation using the operating system's standard C library functions. It intelligently selects the appropriate collation function based on the platform (Windows vs. other systems), database encoding, and locale configuration. On Windows systems with UTF-8 encoding, it delegates to a specialized UTF-8 handling function. For other cases, it uses either locale-specific  or the default  function depending on whether a specific locale is provided.

## Parameters / Member Variables
- : First null-terminated string to compare, encoded in database encoding
- : Second null-terminated string to compare, encoded in database encoding  
- LANG=C.UTF-8
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
LC_ALL=: PostgreSQL locale object containing collation information, or NULL to use database default collation

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_strncoll_libc_win32_utf8](pg_strncoll_libc_win32_utf8.md)
  - strcoll_l
  - strcoll
- Called from (representative examples):
  - [pg_strncoll_libc](pg_strncoll_libc.md)
  - [pg_strcoll](pg_strcoll.md)

## Notes and Other Information
- This is a static function, internal to the PostgreSQL locale handling system
- Includes platform-specific optimizations for Windows UTF-8 handling
- Asserts that if a locale is provided, it must be a libc-based locale provider
- Part of PostgreSQL's abstraction layer for cross-platform locale support