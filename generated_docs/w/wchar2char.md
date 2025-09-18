# wchar2char

## Location
src/backend/utils/adt/pg_locale.c: 3082 - 3137

## Overview
Converts wide character strings (wchar_t) to multibyte character format, providing a PostgreSQL wrapper around the standard wcstombs family of functions.

## Definition
```c
size_t wchar2char(char *to, const wchar_t *from, size_t tolen, pg_locale_t locale)
```

## Detailed Description
The `wchar2char()` function converts wide character strings to multibyte character format, serving as PostgreSQL's wrapper around the standard C library wcstombs functions. It handles different locales and provides special handling for Windows UTF-8 encoding scenarios.

The function follows the same API as the standard `wcstombs_l()` function, requiring the input wide string to be zero-terminated and limiting output to the specified maximum byte length. The output will be zero-terminated only if there is sufficient room in the destination buffer.

Key implementation details:
- On Windows with UTF-8 database encoding, uses `WideCharToMultiByte()` instead of wcstombs due to Windows-specific Unicode handling
- For default locale (NULL), uses standard `wcstombs()`
- For specific locales, uses `wcstombs_l()` with the locale's underlying locale_t
- Only supports LIBC collation provider locales

## Parameters / Member Variables
- `to`: Destination buffer for converted multibyte characters
- `from`: Source wide character string (must be zero-terminated)
- `tolen`: Maximum number of bytes to store in destination buffer
- `locale`: PostgreSQL locale object (must be LIBC provider or NULL for default locale)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (PostgreSQL assertion macro)
  - GetDatabaseEncoding (PostgreSQL function to get current database encoding)
  - WideCharToMultiByte (Windows API function for character conversion)
  - wcstombs (standard C library function for character conversion)
  - wcstombs_l (locale-specific C library function for character conversion)
  - COLLPROVIDER_LIBC (PostgreSQL constant for libc collation provider)
  - PG_UTF8 (PostgreSQL UTF-8 encoding constant)
- Called from (representative examples):
  - lowerstr_with_len (src/backend/tsearch/ts_locale.c:308)
  - str_tolower (src/backend/utils/adt/formatting.c:1747)
  - str_toupper (src/backend/utils/adt/formatting.c:1895)
  - str_initcap (src/backend/utils/adt/formatting.c:2109)
  - get_iso_localename (src/backend/utils/adt/pg_locale.c:1180)

## Notes and Other Information
- Works with libc's wchar_t type, not PostgreSQL's pg_wchar_t type
- Returns the number of bytes written (excluding null terminator) or -1 on error
- On Windows, Microsoft includes the zero terminator in the result count, which is adjusted
- Only supports locales with COLLPROVIDER_LIBC provider
- Part of PostgreSQL's locale and character encoding infrastructure
- Used primarily for text processing functions that need locale-aware case conversion
- Handles Windows-specific UTF-8 encoding issues where standard wcstombs functions don't work correctly
- The output buffer size should account for multibyte character expansion