# builtin_validate_locale

## Location
src/backend/utils/adt/pg_locale.c: 2568 - 2601

## Overview
Validates the locale and encoding combination for the builtin provider and returns the canonical form of the locale name.

## Definition
```c
const char *builtin_validate_locale(int encoding, const char *locale)
```

## Detailed Description
The builtin_validate_locale function performs comprehensive validation of locale and encoding combinations for PostgreSQL's builtin collation provider. It serves two primary purposes: validating that the locale is supported by the builtin provider and ensuring the encoding is compatible with the specified locale.

The function recognizes and canonicalizes locale names:
- "C" remains as "C" (canonical form)
- "C.UTF-8" and "C.UTF8" both canonicalize to "C.UTF-8"

After determining the canonical locale name, it validates the encoding compatibility by calling builtin_locale_encoding() to get the required encoding for the locale. If the locale requires a specific encoding and the provided encoding doesn't match, an error is raised.

This validation is crucial for maintaining data integrity and proper collation behavior in databases using the builtin provider.

## Parameters / Member Variables
- `encoding`: The encoding ID to validate against the locale
- `locale`: The locale name string to validate and canonicalize

## Dependencies
- Functions called/Symbols referenced:
  - builtin_locale_encoding
  - pg_encoding_to_char
  - strcmp (standard C library function)
  - ereport (for error reporting)
  - errcode, ERRCODE_WRONG_OBJECT_TYPE
  - errmsg
- Called from (representative examples):
  - DefineCollation
  - createdb
  - pg_newlocale_from_collation
  - CheckMyDatabase

## Notes and Other Information
- Only supports "C" and "C.UTF-8"/"C.UTF8" locales for builtin provider
- Canonicalizes "C.UTF8" to "C.UTF-8" for consistency
- Performs encoding compatibility validation by checking against required encoding
- Returns the canonical locale name on successful validation
- Raises ERROR for unsupported locales or encoding mismatches
- Used during database creation, collation definition, and locale initialization
- Part of PostgreSQL's builtin collation provider infrastructure
- Essential for ensuring locale/encoding consistency across the system