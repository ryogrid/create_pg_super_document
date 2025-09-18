# char2wchar

## Location
[src/backend/utils/adt/pg_locale.c:3138-3209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L3138-L3209)

## Overview
Converts multibyte character strings to wide character format (wchar_t), providing a PostgreSQL wrapper around the standard mbstowcs family of functions with enhanced error handling.

## Definition
```c
size_t char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen, pg_locale_t locale)
```

## Detailed Description
The `char2wchar()` function converts multibyte character strings to wide character format, serving as PostgreSQL's enhanced wrapper around the standard C library mbstowcs functions. Unlike the standard mbstowcs functions, this function:

1. Accepts byte length instead of requiring null-terminated input
2. Uses ereport() for error handling instead of returning -1
3. Provides better error messages for invalid input encoding
4. Handles Windows-specific UTF-8 encoding scenarios

The function supports different locales and provides special handling for Windows platforms where "Unicode" locales assume UTF-16 encoding. It includes comprehensive error handling that attempts to provide useful error messages by validating the input string and suggesting potential locale/encoding mismatches.

For non-Windows platforms, the function creates a temporary null-terminated copy of the input string since the underlying mbstowcs functions require null termination.

## Parameters / Member Variables
- `to`: Destination buffer for converted wide characters
- `tolen`: Maximum number of wchar_t elements to store in destination buffer
- `from`: Source multibyte character string (not required to be null-terminated)
- `fromlen`: Length in bytes of the source string
- `locale`: PostgreSQL locale object (must be LIBC provider or NULL for default locale)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (PostgreSQL assertion macro)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (PostgreSQL function to get current database encoding)
  - MultiByteToWideChar (Windows API function for character conversion)
  - [pnstrdup](../p/pnstrdup.md) (PostgreSQL function to create null-terminated string copy)
  - mbstowcs (standard C library function for character conversion)
  - [mbstowcs_l](../m/mbstowcs_l.md) (locale-specific C library function for character conversion)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [pg_verifymbstr](../p/pg_verifymbstr.md) (PostgreSQL function to validate multibyte strings)
  - ereport (PostgreSQL error reporting)
  - COLLPROVIDER_LIBC (PostgreSQL constant for libc collation provider)
  - PG_UTF8 (PostgreSQL UTF-8 encoding constant)
- Called from (representative examples):
  - [t_isdigit](../t/t_isdigit.md), t_isspace, t_isalpha, t_isalnum, t_isprint (text search locale functions)
  - [lowerstr_with_len](../l/lowerstr_with_len.md) (src/backend/tsearch/ts_locale.c:293)
  - TParserInit (src/backend/tsearch/wparser_def.c:317)
  - [str_tolower](../s/str_tolower.md), str_toupper, str_initcap (formatting functions)

## Notes and Other Information
- Works with libc's wchar_t type, not PostgreSQL's pg_wchar_t type
- Returns the number of wide characters written (excluding null terminator) or calls ereport() on error
- Automatically null-terminates output when there is room in the destination buffer
- On Windows with UTF-8, uses MultiByteToWideChar() instead of mbstowcs due to encoding issues
- Creates temporary null-terminated copies on non-Windows platforms since mbstowcs requires null termination
- Provides enhanced error reporting with hints about potential LC_CTYPE/database encoding mismatches
- Only supports locales with COLLPROVIDER_LIBC provider
- Part of PostgreSQL's locale and character encoding infrastructure
- Used extensively by text search and formatting functions for locale-aware character processing
- Input validation via pg_verifymbstr() helps distinguish encoding errors from locale mismatches