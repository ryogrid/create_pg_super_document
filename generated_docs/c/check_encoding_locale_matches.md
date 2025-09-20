# check_encoding_locale_matches

## Location
[src/backend/commands/dbcommands.c:1557-1594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L1557-L1594)

## Overview
check_encoding_locale_matches validates that the chosen character encoding is compatible with the specified locale settings to prevent data corruption from locale-encoding mismatches.

## Definition

```c
void
check_encoding_locale_matches(int encoding, const char *collate, const char *ctype)
```
## Detailed Description
This function enforces encoding-locale compatibility requirements by checking that the selected character encoding matches what the specified LC_COLLATE and LC_CTYPE locale settings expect. Since libc's locale-specific code typically fails when presented with data in an unexpected encoding, this validation prevents runtime errors and data corruption.

The function implements a policy with four specific exceptions where encoding-locale mismatches are permitted: SQL_ASCII locale (C/POSIX) works with any encoding, unknown locale encoding (-1) trusts user judgment, UTF8 encoding on Windows platforms (converted to UTF16 internally), and SQL_ASCII encoding for superusers (historically allowed despite being risky).

Both LC_COLLATE and LC_CTYPE are validated separately against the chosen encoding, with detailed error messages indicating which locale setting is incompatible and what encoding would be required.

## Parameters / Member Variables
- : The character encoding ID selected for the database
- : The LC_COLLATE locale name string to validate against
- : The LC_CTYPE locale name string to validate against

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_encoding_from_locale
  - pg_encoding_to_char
  - superuser
  - Constants: PG_SQL_ASCII, PG_UTF8
- Called from (representative examples):
  - [createdb](createdb.md)
  - [DefineCollation](../D/DefineCollation.md)

## Notes and Other Information
- Policy must be kept synchronized with initdb to ensure consistency during database cluster initialization
- Windows platform has special UTF8 handling due to internal UTF16 conversion
- SQL_ASCII encoding exception for superusers is maintained for historical compatibility and regression test requirements
- The restriction exists because libc locale functions fail with unexpected encodings, potentially causing crashes or data corruption
- Returns detailed error messages specifying both the conflicting locale and the required encoding for that locale