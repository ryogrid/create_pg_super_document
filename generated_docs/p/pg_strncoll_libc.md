# pg_strncoll_libc

## Location
src/backend/utils/adt/pg_locale.c: 1968 - 2019

## Overview
Performs locale-aware string collation for non-null-terminated strings by creating null-terminated copies and delegating to pg_strcoll_libc.

## Definition
```c
static int pg_strncoll_libc(const char *arg1, size_t len1, const char *arg2, size_t len2, pg_locale_t locale)
```

## Detailed Description
The `pg_strncoll_libc` function handles string collation for strings that may not be null-terminated by first creating null-terminated copies of the input strings and then calling `pg_strcoll_libc` to perform the actual collation. It uses a stack-based buffer for small strings to avoid dynamic allocation overhead, but falls back to palloc() for larger strings. On Windows with UTF-8 encoding, it optimizes by directly calling the specialized UTF-8 handler without creating temporary copies.

## Parameters / Member Variables
- `arg1`: First string to compare (may not be null-terminated)
- `len1`: Length of the first string in bytes
- `arg2`: Second string to compare (may not be null-terminated)
- `len2`: Length of the second string in bytes
- `locale`: PostgreSQL locale object for collation rules, or NULL for database default

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_strncoll_libc_win32_utf8](pg_strncoll_libc_win32_utf8.md)
  - [palloc](palloc.md)
  - memcpy
  - [pg_strcoll_libc](pg_strcoll_libc.md)
  - [pfree](pfree.md)
- Called from (representative examples):
  - [pg_strncoll](pg_strncoll.md)

## Notes and Other Information
- Static function internal to PostgreSQL's locale system
- Uses TEXTBUFLEN constant to determine when to use stack vs. heap allocation
- Includes Windows-specific optimization for UTF-8 encoding
- Properly manages memory by freeing dynamically allocated buffers
- Part of the collation abstraction layer that handles both null-terminated and length-specified strings