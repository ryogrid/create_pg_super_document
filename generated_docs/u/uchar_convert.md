# uchar_convert

## Location
src/backend/utils/adt/pg_locale.c: 2731 - 2757

## Overview
Converts a string from the source encoding to ICU's Unicode (UChar) representation, storing the result in a provided destination buffer.

## Definition
static int32_t uchar_convert(UConverter *converter, UChar *dest, int32_t destlen, const char *src, int32_t srclen)

## Detailed Description
This function performs the actual character encoding conversion from PostgreSQL's database encoding to ICU's Unicode representation. It takes a source string and converts it to a UChar array (16-bit Unicode code units) using the provided ICU converter. Unlike uchar_length which only calculates space requirements, this function performs the actual conversion and stores the result.

The function wraps ICU's ucnv_toUChars() function with PostgreSQL-style error handling, ensuring that conversion failures are properly reported through PostgreSQL's error reporting mechanism. It returns the actual length of the converted string, which may be useful for further processing.

## Parameters / Member Variables
- : UConverter instance configured for the source encoding to Unicode conversion
- : Destination buffer to store the converted UChar string
- : Size of the destination buffer in UChar units
- : Source string in the database encoding to convert
- : Length of the source string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ucnv_toUChars (ICU function to perform the actual string conversion)
  - u_errorName (ICU function to get error name string for error reporting)
- Called from (representative examples):
  - collation_cache_entry (during collation setup)
  - [pg_strncoll_icu_no_utf8](../p/pg_strncoll_icu_no_utf8.md) (string comparison operations)
  - [pg_strnxfrm_icu](../p/pg_strnxfrm_icu.md) (string transformation operations)
  - [pg_strnxfrm_prefix_icu_no_utf8](../p/pg_strnxfrm_prefix_icu_no_utf8.md) (prefix transformation)
  - [icu_to_uchar](../i/icu_to_uchar.md) (character conversion helper)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c file
- Returns int32_t representing the actual length of the converted string in UChar units
- Assumes the destination buffer is properly sized (typically determined by calling uchar_length first)
- Error handling follows PostgreSQL conventions using ereport()
- The function performs the complement operation to icu_from_uchar
- Critical for enabling ICU-based collation and string operations in PostgreSQL
- Buffer overflow protection relies on proper size calculation beforehand
- The converted string in the destination buffer is ready for use with ICU string functions