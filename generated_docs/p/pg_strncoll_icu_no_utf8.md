# pg_strncoll_icu_no_utf8

## Location
[src/backend/utils/adt/pg_locale.c:2020-2074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2020-L2074)

## Overview
Performs ICU-based string collation for non-UTF8 database encodings by converting input strings to UChar format and using ICU's ucol_strcoll function.

## Definition
```c
static int pg_strncoll_icu_no_utf8(const char *arg1, int32_t len1, const char *arg2, int32_t len2, pg_locale_t locale)
```

## Detailed Description
The `pg_strncoll_icu_no_utf8` function handles ICU-based string collation for cases where the database encoding is not UTF-8 or ICU doesn't support the UTF-8 optimization. It first converts both input strings from the database encoding to ICU's internal UChar (UTF-16) representation, then performs the collation using ICU's `ucol_strcoll` function. The function manages memory efficiently by using a stack buffer for small strings and dynamically allocating memory only when needed for larger strings.

## Parameters / Member Variables
- `arg1`: First string to compare, encoded in database encoding
- `len1`: Length of first string in bytes, or -1 if null-terminated
- `arg2`: Second string to compare, encoded in database encoding  
- `len2`: Length of second string in bytes, or -1 if null-terminated
- `locale`: PostgreSQL locale object containing ICU collation information

## Dependencies
- Functions called/Symbols referenced:
  - [init_icu_converter](../i/init_icu_converter.md)
  - [uchar_length](../u/uchar_length.md)
  - [uchar_convert](../u/uchar_convert.md)
  - ucol_strcoll
  - [palloc](palloc.md)
  - [pfree](pfree.md)
- Called from (representative examples):
  - [pg_strncoll_icu](pg_strncoll_icu.md)

## Notes and Other Information
- Static function used internally within PostgreSQL's ICU collation system
- Only used when database encoding is not UTF-8 or when HAVE_UCOL_STRCOLLUTF8 is not available
- Converts strings to ICU's internal UChar (UTF-16) representation for accurate collation
- Uses efficient memory management with stack allocation for small strings
- Part of PostgreSQL's ICU integration for international collation support
- Asserts that the locale provider is COLLPROVIDER_ICU to ensure correct usage