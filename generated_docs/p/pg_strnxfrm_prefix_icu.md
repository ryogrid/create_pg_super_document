# pg_strnxfrm_prefix_icu

## Location
src/backend/utils/adt/pg_locale.c: 2321 - 2371

## Overview
Generates a partial sort key prefix for string collation using the ICU (International Components for Unicode) library provider, supporting both UTF-8 and non-UTF-8 database encodings.

## Definition
```c
static size_t pg_strnxfrm_prefix_icu(char *dest, const char *src, int32_t srclen,
                                   int32_t destsize, pg_locale_t locale)
```

## Detailed Description
This function creates a sort key prefix for a given string using ICU collation rules. It handles two different paths depending on the database encoding:
- For UTF-8 databases, it uses ICU's `ucol_nextSortKeyPart` function directly with a UCharIterator
- For non-UTF-8 databases, it delegates to `pg_strnxfrm_prefix_icu_no_utf8` helper function

The function is part of PostgreSQL's collation infrastructure and is used internally for generating sort keys that enable efficient string comparison and sorting operations according to locale-specific rules.

## Parameters / Member Variables
- `dest`: Output buffer where the generated sort key prefix will be stored
- `src`: Input string to transform (can be NULL-terminated if srclen is -1)
- `srclen`: Length of the source string, or -1 if the string is NULL-terminated
- `destsize`: Size of the destination buffer in bytes
- `locale`: PostgreSQL locale structure that must use ICU as the collation provider

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - ucol_nextSortKeyPart (ICU function)
  - uiter_setUTF8 (ICU function)
  - [pg_strnxfrm_prefix_icu_no_utf8](pg_strnxfrm_prefix_icu_no_utf8.md)
  - ereport
  - u_errorName (ICU function)
- Called from (representative examples):
  - [pg_strxfrm_prefix](pg_strxfrm_prefix.md)
  - [pg_strnxfrm_prefix](pg_strnxfrm_prefix.md)

## Notes and Other Information
- This is a static function only accessible within pg_locale.c
- Requires the locale provider to be COLLPROVIDER_ICU (enforced by assertion)
- Handles ICU errors by reporting them through PostgreSQL's error reporting system
- The function optimizes for UTF-8 encoding by using ICU's native UTF-8 iterator
- Sort key generation is essential for locale-aware string comparison and indexing operations