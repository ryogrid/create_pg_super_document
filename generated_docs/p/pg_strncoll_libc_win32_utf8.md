# pg_strncoll_libc_win32_utf8

## Location
src/backend/utils/adt/pg_locale.c: 1862 - 1938

## Overview
A Windows-specific static function that performs locale-aware string collation for UTF-8 encoded strings using the libc provider by converting to UTF-16 and using Windows wcscoll functions.

## Definition


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