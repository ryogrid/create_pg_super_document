# str_tolower

## Location
[src/backend/utils/adt/formatting.c:1636-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1636-L1783)

## Overview
A collation-aware, wide-character-aware function that converts a string to lowercase, supporting multiple collation providers including ICU, built-in Unicode, and libc.

## Definition
```c
char *str_tolower(const char *buff, size_t nbytes, Oid collid)
```

## Detailed Description
The `str_tolower` function provides robust lowercase conversion functionality that respects database collation settings. It handles multiple encoding scenarios and collation providers:

1. **C/POSIX Collations**: Uses ASCII-only conversion via `asc_tolower`
2. **ICU Provider**: Leverages ICU library functions for Unicode-aware case conversion
3. **Built-in Provider**: Uses PostgreSQL's internal Unicode conversion for UTF-8 databases
4. **libc Provider**: Falls back to system locale functions, with special handling for multibyte encodings

The function automatically detects the appropriate conversion method based on the collation and database encoding, ensuring correct case conversion across different locales and character sets.

## Parameters / Member Variables
- `buff`: Input string buffer to convert (can be null)
- `nbytes`: Number of bytes in the input buffer
- `collid`: OID of the collation to use for case conversion

## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md): Check if collation uses C/POSIX locale
  - [asc_tolower](../a/asc_tolower.md): ASCII-only lowercase conversion
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md): Get locale information from collation OID
  - [icu_to_uchar](../i/icu_to_uchar.md), `icu_convert_case`, `icu_from_uchar`: ICU conversion functions
  - [unicode_strlower](../u/unicode_strlower.md): Built-in Unicode lowercase conversion
  - [char2wchar](../c/char2wchar.md), `wchar2char`: Wide character conversion functions
  - `towlower_l`, `tolower_l`: Locale-aware case conversion
  - [pg_tolower](../p/pg_tolower.md): PostgreSQL's ASCII case conversion
- Called from (representative examples):
  - [lower](../l/lower.md): SQL LOWER() function implementation
  - [seq_search_localized](seq_search_localized.md): Localized pattern searching
  - [str_tolower_z](str_tolower_z.md): Null-terminated string wrapper

## Notes and Other Information
- Returns a palloc'd, null-terminated string that must be freed by the caller
- Throws an error if collation OID is invalid or indeterminate
- For multibyte encodings with libc provider, uses wide character functions to ensure proper handling
- The function assumes database character encoding matches LC_CTYPE encoding
- Memory allocation is carefully managed with overflow protection for large strings
- Special handling ensures ASCII I/i behavior in default collations while respecting locale-specific rules in non-default collations