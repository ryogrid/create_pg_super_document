# pg_strncoll_icu

## Location
src/backend/utils/adt/pg_locale.c: 2075 - 2120

## Overview
Provides ICU-based string collation with automatic optimization for UTF-8 database encodings, selecting the most efficient ICU collation function available.

## Definition
```c
static int pg_strncoll_icu(const char *arg1, int32_t len1, const char *arg2, int32_t len2, pg_locale_t locale)
```

## Detailed Description
The `pg_strncoll_icu` function serves as the primary interface for ICU-based string collation in PostgreSQL. It intelligently chooses between two collation strategies: when the database encoding is UTF-8 and ICU supports the optimized `ucol_strcollUTF8` function, it uses that for better performance by avoiding character encoding conversion. Otherwise, it delegates to `pg_strncoll_icu_no_utf8` which handles the conversion to ICU's internal UChar format. The function includes proper error handling for ICU failures and reports detailed error messages using ICU's error reporting mechanisms.

## Parameters / Member Variables
- `arg1`: First string to compare, encoded in database encoding
- `len1`: Length of first string in bytes, or -1 if null-terminated
- `arg2`: Second string to compare, encoded in database encoding
- `len2`: Length of second string in bytes, or -1 if null-terminated
- `locale`: PostgreSQL locale object containing ICU collation configuration

## Dependencies
- Functions called/Symbols referenced:
  - GetDatabaseEncoding
  - ucol_strcollUTF8
  - u_errorName
  - ereport
  - pg_strncoll_icu_no_utf8
- Called from (representative examples):
  - pg_strcoll
  - pg_strncoll

## Notes and Other Information
- Static function serving as the main ICU collation dispatcher
- Provides UTF-8 optimization when both database and ICU support it
- Includes comprehensive error handling with ICU-specific error reporting
- Part of PostgreSQL's ICU integration for international text processing
- Asserts that the locale uses COLLPROVIDER_ICU to ensure correct usage context
- Conditionally compiled features based on ICU capabilities (HAVE_UCOL_STRCOLLUTF8)