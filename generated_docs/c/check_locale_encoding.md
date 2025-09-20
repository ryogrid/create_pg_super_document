# check_locale_encoding

## Location
[src/bin/initdb/initdb.c:2247-2281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2247-L2281)

## Overview
Validates that the user-specified encoding is compatible with the encoding required by a given locale.

## Definition

```c
static bool
check_locale_encoding(const char *locale, int user_enc)
```
## Detailed Description
This function ensures compatibility between a user-specified character encoding and the encoding naturally used by a system locale. It prevents encoding mismatches that would lead to incorrect character string processing and data corruption in PostgreSQL. The function implements the same validation logic as the backend createdb() function to maintain consistency across PostgreSQL components.

The validation includes several special cases: SQL_ASCII is considered compatible with any locale, unknown locale encodings (-1) are accepted, and on Windows systems, UTF8 encoding is allowed with any locale due to platform-specific character handling behaviors.

## Parameters / Member Variables
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
LC_ALL=: The locale name string to check encoding compatibility against
- : The PostgreSQL encoding identifier that the user wants to use

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_encoding_from_locale (determines the encoding naturally used by the locale)
  - pg_encoding_to_char (converts encoding IDs to human-readable names)
  - pg_log_error (logs error messages)
  - pg_log_error_detail (provides detailed error explanations)
  - pg_log_error_hint (offers suggestions for resolution)
  - PG_SQL_ASCII, PG_UTF8 (encoding constants)
- Called from (representative examples):
  - [setup_locale_encoding](../s/setup_locale_encoding.md) (during database initialization encoding setup)

## Notes and Other Information
- Returns true if encodings are compatible, false if they conflict
- Includes Windows-specific exception allowing UTF8 with any locale
- Provides detailed error messages explaining the mismatch and suggesting solutions
- The compatibility rules match those used in the backend createdb() function
- SQL_ASCII encoding is universally compatible as a fallback option
- Unknown locale encodings (return value -1) are accepted to handle edge cases
- Part of the database initialization process to prevent character encoding issues
- Essential for preventing data corruption from encoding mismatches