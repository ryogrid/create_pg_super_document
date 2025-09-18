# pg_strxfrm

## Location
src/backend/utils/adt/pg_locale.c: 2404 - 2439

## Overview
Transforms a null-terminated string into a sort key that can be compared using ordinary strcmp() instead of locale-aware comparison functions.

## Definition
```c
size_t pg_strxfrm(char *dest, const char *src, size_t destsize, pg_locale_t locale)
```

## Detailed Description
This function provides PostgreSQL's main interface for string transformation that enables efficient locale-aware sorting. It converts a source string into a binary sort key such that lexicographic comparison (strcmp) of the transformed strings produces the same ordering as locale-aware comparison (pg_strcoll) of the original strings.

The function serves as a dispatcher that routes to the appropriate implementation based on the collation provider:
- **LIBC provider**: Uses pg_strxfrm_libc() which wraps the system's strxfrm() function
- **ICU provider**: Uses pg_strnxfrm_icu() with srclen=-1 to indicate null-terminated input

This transformation is particularly valuable for sorting operations where many comparisons are needed, as it allows the expensive locale-aware comparison to be done once per string rather than for every comparison pair.

## Parameters / Member Variables
- `dest`: Output buffer for the transformed sort key (can be NULL if destsize is 0)
- `src`: Input null-terminated string to transform
- `destsize`: Size of the destination buffer in bytes
- `locale`: PostgreSQL locale structure specifying collation rules (NULL means C locale)

## Dependencies
- Functions called/Symbols referenced:
  - pg_strxfrm_libc
  - pg_strnxfrm_icu
  - COLLPROVIDER_LIBC
  - COLLPROVIDER_ICU
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - varstr_abbrev_convert
  - Various sorting and indexing operations

## Notes and Other Information
- Returns the number of bytes needed to store the complete transformed string (excluding null terminator)
- If the return value is >= destsize, the contents of dest are undefined (similar to strxfrm behavior)
- The function requires src to be null-terminated, unlike pg_strnxfrm which accepts a length parameter
- Transformed strings are only valid for comparison with other strings transformed using the same locale
- This is a key component in PostgreSQL's collation infrastructure for optimizing text sorting performance