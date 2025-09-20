# pg_strnxfrm_prefix_icu_no_utf8

## Location
[src/backend/utils/adt/pg_locale.c:2273-2320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2273-L2320)

## Overview
This static function generates partial ICU sort keys for non-UTF8 databases using ICU's incremental sort key generation capabilities.

## Definition

```c
static size_t
pg_strnxfrm_prefix_icu_no_utf8(char *dest, const char *src, int32_t srclen,
							   int32_t destsize, pg_locale_t locale)
```
## Detailed Description
pg_strnxfrm_prefix_icu_no_utf8 is a specialized function that generates partial sort keys using ICU's ucol_nextSortKeyPart() function. This function is specifically designed for non-UTF8 database encodings and enables incremental sort key generation, which is useful for prefix operations and memory-constrained scenarios.

The function performs the following operations:
1. Converts the input string from database encoding to Unicode (UChar)
2. Initializes a UCharIterator for the Unicode string
3. Uses ucol_nextSortKeyPart() to generate a partial sort key
4. Handles ICU errors with appropriate error reporting
5. Returns the size of the generated sort key part

This approach allows for generating sort key prefixes without needing to process the entire string, which can be more efficient for certain operations like range queries or when working with limited buffer space.

## Parameters / Member Variables
- : Buffer to store the partial sort key
- : Source string in database encoding
- : Length of source string (-1 indicates null-terminated)
- : Size of destination buffer
- LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: ICU locale specification with collation rules

## Dependencies
- Functions called/Symbols referenced:
  - [init_icu_converter](../i/init_icu_converter.md)
  - [uchar_length](../u/uchar_length.md)
  - [uchar_convert](../u/uchar_convert.md)
  - uiter_setString
  - ucol_nextSortKeyPart
  - u_errorName
  - ereport
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [palloc](palloc.md)
  - [pfree](pfree.md)
  - COLLPROVIDER_ICU
  - PG_UTF8
  - TEXTBUFLEN
- Called from (representative examples):
  - [pg_strnxfrm_prefix_icu](pg_strnxfrm_prefix_icu.md) (src/backend/utils/adt/pg_locale.c:2349)

## Notes and Other Information
- This is a static function available only when ICU support is compiled
- Specifically designed for non-UTF8 database encodings (includes assertion check)
- Uses ICU's incremental sort key generation (ucol_nextSortKeyPart) instead of full transformation
- Includes comprehensive error handling with ICU status reporting
- The function initializes and manages UCharIterator state for incremental processing
- Uses stack buffer optimization for small strings to minimize memory allocation
- Unlike full sort key generation, this may not produce complete sort keys suitable for all comparison scenarios
- Located in src/backend/utils/adt/pg_locale.c:2273-2320