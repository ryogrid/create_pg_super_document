# uchar_length

## Location
[src/backend/utils/adt/pg_locale.c:2714-2730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2714-L2730)

## Overview
Calculates the length in UChar units that a given string would occupy when converted from the source encoding to ICU's Unicode representation.

## Definition
static size_t uchar_length(UConverter *converter, const char *str, int32_t len)

## Detailed Description
This utility function determines how many UChar (16-bit Unicode code units) would be needed to represent a given string after conversion from the source encoding to ICU's Unicode format. It uses ICU's ucnv_toUChars() function with a NULL destination buffer to perform a "dry run" conversion that only calculates the required buffer size without actually performing the conversion.

The function is essential for memory allocation planning when converting strings from PostgreSQL's database encoding to ICU's Unicode representation. By knowing the target length in advance, calling code can allocate appropriately sized buffers before performing the actual conversion.

## Parameters / Member Variables
- : UConverter instance configured for the source encoding to Unicode conversion
- : Source string in the database encoding to measure
- : Length of the source string in bytes (can be -1 for null-terminated strings)

## Dependencies
- Functions called/Symbols referenced:
  - ucnv_toUChars (ICU function to convert string, called with NULL buffer to get size)
  - u_errorName (ICU function to get error name string for error reporting)
- Called from (representative examples):
  - collation_cache_entry (during collation setup)
  - [pg_strncoll_icu_no_utf8](../p/pg_strncoll_icu_no_utf8.md) (string comparison operations)
  - [pg_strnxfrm_icu](../p/pg_strnxfrm_icu.md) (string transformation operations)
  - [pg_strnxfrm_prefix_icu_no_utf8](../p/pg_strnxfrm_prefix_icu_no_utf8.md) (prefix transformation)
  - [icu_to_uchar](../i/icu_to_uchar.md) (character conversion helper)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c file
- Returns size_t representing the number of UChar units needed
- The function expects U_BUFFER_OVERFLOW_ERROR when called with NULL buffer - this is the normal way ICU indicates buffer size requirements
- Error handling follows PostgreSQL conventions using ereport()
- This is a measurement function that doesn't modify any data - it only calculates space requirements
- Critical for preventing buffer overflows in ICU string conversion operations
- The returned length includes space for null termination when applicable