# pg_strncoll_libc_win32_utf8

## Location
[src/backend/utils/adt/pg_locale.c:1862-1938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1862-L1938)

## Overview
A Windows-specific static function that performs locale-aware string collation for UTF-8 encoded strings using the libc provider by converting to UTF-16 and using Windows wcscoll functions.

## Definition

```c
static int
pg_strncoll_libc_win32_utf8(const char *arg1, size_t len1, const char *arg2,
							size_t len2, pg_locale_t locale)
```
## Detailed Description
This function implements UTF-8 string collation on Windows systems using the libc collation provider. Since Windows libc functions work with UTF-16 (wide characters), the function converts the input UTF-8 strings to UTF-16 using MultiByteToWideChar, then performs the collation using either wcscoll_l (with locale) or wcscoll (default locale). It handles buffer allocation efficiently using a stack buffer (TEXTBUFLEN) for small strings and dynamic allocation for larger ones. The function includes comprehensive error handling for UTF-8 to UTF-16 conversion failures and collation errors, and properly manages memory cleanup.

## Parameters / Member Variables
- `arg1`: First UTF-8 string to compare
- `len1`: Length of the first string in bytes
- `arg2`: Second UTF-8 string to compare  
- `len2`: Length of the second string in bytes
- `locale`: PostgreSQL locale object specifying the collation rules, or NULL for default locale

## Dependencies
- Functions called/Symbols referenced:
  - MultiByteToWideChar (Windows API for UTF-8 to UTF-16 conversion)
  - wcscoll_l, wcscoll (Windows wide character collation functions)
  - GetLastError (Windows error reporting)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (PostgreSQL encoding function)
  - [palloc](palloc.md), pfree (PostgreSQL memory management)
  - ereport, errmsg (PostgreSQL error reporting)
  - TEXTBUFLEN, PG_UTF8, COLLPROVIDER_LIBC (constants)
- Called from (representative examples):
  - [pg_strcoll_libc](pg_strcoll_libc.md) (at line 1950)
  - [pg_strncoll_libc](pg_strncoll_libc.md) (at line 1984)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c compilation unit
- Windows-only implementation (includes Assert(false) on non-Windows platforms)
- Requires database encoding to be UTF-8 (enforced by assertion)
- Handles zero-length strings as a special case since Windows API doesn't work with them
- Uses efficient memory management with stack buffer for small strings, heap allocation for large ones
- Properly null-terminates the UTF-16 converted strings as required by wcscoll functions
- Returns standard collation result: negative if arg1 < arg2, zero if equal, positive if arg1 > arg2
- Includes specific error handling for the _NLSCMPERROR return value (2147483647) from wcscoll

## Simplified Source

```c
static int
pg_strncoll_libc_win32_utf8(const char *arg1, size_t len1, const char *arg2,
                            size_t len2, pg_locale_t locale)
{
    char sbuf[TEXTBUFLEN];
    char *buf = sbuf;
    char *utf16_arg1, *utf16_arg2;
    int buf1_size = len1 * 2 + 2;
    int buf2_size = len2 * 2 + 2;
    int result;

    // Allocate buffer space for UTF-16 conversion
    if (buf1_size + buf2_size > TEXTBUFLEN)
        buf = palloc(buf1_size + buf2_size);

    utf16_arg1 = buf;
    utf16_arg2 = buf + buf1_size;

    // Convert first string from UTF-8 to UTF-16
    if (len1 == 0) {
        ((LPWSTR) utf16_arg1)[0] = 0;
    } else {
        int chars = MultiByteToWideChar(CP_UTF8, 0, arg1, len1,
                                        (LPWSTR) utf16_arg1, buf1_size / 2);
        if (!chars)
            ereport(ERROR, (errmsg("UTF-8 to UTF-16 conversion failed")));
        ((LPWSTR) utf16_arg1)[chars] = 0;
    }

    // Convert second string from UTF-8 to UTF-16
    if (len2 == 0) {
        ((LPWSTR) utf16_arg2)[0] = 0;
    } else {
        int chars = MultiByteToWideChar(CP_UTF8, 0, arg2, len2,
                                        (LPWSTR) utf16_arg2, buf2_size / 2);
        if (!chars)
            ereport(ERROR, (errmsg("UTF-8 to UTF-16 conversion failed")));
        ((LPWSTR) utf16_arg2)[chars] = 0;
    }

    // Perform wide character collation
    if (locale)
        result = wcscoll_l((LPWSTR) utf16_arg1, (LPWSTR) utf16_arg2, locale->info.lt);
    else
        result = wcscoll((LPWSTR) utf16_arg1, (LPWSTR) utf16_arg2);

    // Check for collation error
    if (result == 2147483647) // _NLSCMPERROR
        ereport(ERROR, (errmsg("Unicode string comparison failed")));

    // Clean up allocated memory
    if (buf != sbuf)
        pfree(buf);

    return result;
}
```