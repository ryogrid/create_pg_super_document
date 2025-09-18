# str_initcap

## Location
src/backend/utils/adt/formatting.c: 1973 - 2157

## Overview
A collation-aware, wide-character-aware function that converts the first letter of each word to uppercase and the rest to lowercase, supporting multiple collation providers including ICU, built-in Unicode, and libc.

## Definition
```c
char *str_initcap(const char *buff, size_t nbytes, Oid collid)
```

## Detailed Description
The `str_initcap` function implements initial capitalization (title case) functionality that respects database collation settings. It capitalizes the first letter of each word while converting all other letters to lowercase. The function handles multiple encoding scenarios and collation providers:

1. **C/POSIX Collations**: Uses ASCII-only conversion via `asc_initcap`
2. **ICU Provider**: Leverages ICU library functions (`u_strToTitle_default_BI`) for Unicode-aware title case conversion
3. **Built-in Provider**: Uses PostgreSQL's internal Unicode conversion (`unicode_strtitle`) with custom word boundary detection via `initcap_wbnext`
4. **libc Provider**: Implements character-by-character processing using wide character functions for multibyte encodings, or byte-by-byte for single-byte encodings

The function uses a `wasalnum` flag to track whether the previous character was alphanumeric, allowing it to determine when to capitalize (first character of a word) versus when to make lowercase (subsequent characters in a word).

## Parameters / Member Variables
- `buff`: Input string buffer to convert (can be null)
- `nbytes`: Number of bytes in the input buffer
- `collid`: OID of the collation to use for case conversion and word boundary detection

## Dependencies
- Functions called/Symbols referenced:
  - `[lc_ctype_is_c](../l/lc_ctype_is_c.md)`: Check if collation uses C/POSIX locale
  - `[asc_initcap](../a/asc_initcap.md)`: ASCII-only initial capitalization
  - `[pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)`: Get locale information from collation OID
  - `[icu_to_uchar](../i/icu_to_uchar.md)`, `icu_convert_case`, `icu_from_uchar`: ICU conversion functions
  - `[unicode_strtitle](../u/unicode_strtitle.md)`: Built-in Unicode title case conversion
  - `[initcap_wbnext](../i/initcap_wbnext.md)`: Custom word boundary iterator for built-in provider
  - `[WordBoundaryState](../W/WordBoundaryState.md)`: State structure for word boundary detection
  - `[char2wchar](../c/char2wchar.md)`, `wchar2char`: Wide character conversion functions
  - `towlower_l`, `towupper_l`, `iswalnum_l`: Locale-aware wide character functions
  - `tolower_l`, `toupper_l`, `isalnum_l`: Locale-aware character functions
  - `[pg_tolower](../p/pg_tolower.md)`, `pg_toupper`: PostgreSQL's ASCII case conversion
- Called from (representative examples):
  - `[initcap](../i/initcap.md)`: SQL INITCAP() function implementation
  - `[str_initcap_z](str_initcap_z.md)`: Null-terminated string wrapper

## Notes and Other Information
- Returns a palloc'd, null-terminated string that must be freed by the caller
- Throws an error if collation OID is invalid or indeterminate
- For multibyte encodings with libc provider, uses wide character functions to ensure proper handling
- The function assumes database character encoding matches LC_CTYPE encoding
- Memory allocation is carefully managed with overflow protection for large strings
- Built-in provider uses a custom word boundary iterator (`initcap_wbnext`) that defines word boundaries as transitions between alphanumeric and non-alphanumeric characters
- The algorithm maintains state between characters to track word boundaries and apply appropriate case conversion
- ICU provider uses the most sophisticated word boundary detection following Unicode standards