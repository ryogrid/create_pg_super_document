# pg_strncoll_libc

## Location
[src/backend/utils/adt/pg_locale.c:1968-2019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1968-L2019)

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

## Simplified Source

```c
static int
pg_strncoll_libc(const char *arg1, size_t len1, const char *arg2, size_t len2,
                 pg_locale_t locale)
{
    char sbuf[TEXTBUFLEN];
    char *buf = sbuf;
    size_t bufsize1 = len1 + 1;
    size_t bufsize2 = len2 + 1;
    char *arg1n;
    char *arg2n;
    int result;

    Assert(!locale || locale->provider == COLLPROVIDER_LIBC);

#ifdef WIN32
    // Windows UTF-8 optimization - avoid null-termination overhead
    if (GetDatabaseEncoding() == PG_UTF8)
        return pg_strncoll_libc_win32_utf8(arg1, len1, arg2, len2, locale);
#endif

    // Use heap allocation if strings are too large for stack buffer
    if (bufsize1 + bufsize2 > TEXTBUFLEN)
        buf = palloc(bufsize1 + bufsize2);

    // Set up null-terminated string pointers
    arg1n = buf;
    arg2n = buf + bufsize1;

    // Create null-terminated copies of input strings
    memcpy(arg1n, arg1, len1);
    arg1n[len1] = '\0';
    memcpy(arg2n, arg2, len2);
    arg2n[len2] = '\0';

    // Perform actual collation comparison
    result = pg_strcoll_libc(arg1n, arg2n, locale);

    // Clean up heap allocation if used
    if (buf != sbuf)
        pfree(buf);

    return result;
}
```