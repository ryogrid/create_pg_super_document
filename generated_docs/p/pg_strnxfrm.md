# pg_strnxfrm

## Location
src/backend/utils/adt/pg_locale.c: 2440 - 2462

## Overview
Transforms a string of specified length into a sort key that can be compared using ordinary strcmp() instead of locale-aware comparison, without requiring null-terminated input.

## Definition
```c
size_t pg_strnxfrm(char *dest, size_t destsize, const char *src, size_t srclen,
                  pg_locale_t locale)
```

## Detailed Description
This function provides PostgreSQL's length-aware interface for string transformation, similar to pg_strxfrm() but accepting strings that are not necessarily null-terminated. It converts a source string of specified length into a binary sort key that enables efficient locale-aware sorting through standard lexicographic comparison.

The function serves as a dispatcher routing to appropriate implementations based on collation provider:
- **LIBC provider**: Uses pg_strnxfrm_libc() which may need to create a null-terminated copy for libc functions
- **ICU provider**: Uses pg_strnxfrm_icu() which can handle non-null-terminated strings natively

This variant is particularly useful when working with text data that may contain embedded nulls or when the string length is already known, avoiding the need to scan for null terminators.

## Parameters / Member Variables
- `dest`: Output buffer for the transformed sort key (can be NULL if destsize is 0)  
- `destsize`: Size of the destination buffer in bytes
- `src`: Input string to transform (does not need to be null-terminated)
- `srclen`: Length of the source string in bytes
- `locale`: PostgreSQL locale structure specifying collation rules (NULL means C locale)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strnxfrm_libc](pg_strnxfrm_libc.md)
  - [pg_strnxfrm_icu](pg_strnxfrm_icu.md)  
  - COLLPROVIDER_LIBC
  - COLLPROVIDER_ICU
  - PGLOCALE_SUPPORT_ERROR
- Called from (representative examples):
  - [hashtext](../h/hashtext.md)
  - [hashtextextended](../h/hashtextextended.md)
  - [hashbpchar](../h/hashbpchar.md)
  - [hashbpcharextended](../h/hashbpcharextended.md)

## Notes and Other Information
- Returns the number of bytes needed to store the complete transformed string (excluding null terminator)
- If return value is >= destsize, the contents of dest are undefined
- More flexible than pg_strxfrm() as it doesn't require null-terminated input
- Heavily used in PostgreSQL's hash functions for text types to ensure consistent hashing across different locales
- The function comment specifically notes that callers with null-terminated strings should prefer pg_strxfrm() for efficiency
- Critical component for locale-aware hashing and indexing operations