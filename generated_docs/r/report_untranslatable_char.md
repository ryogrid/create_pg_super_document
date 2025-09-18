# report_untranslatable_char

## Location
src/backend/utils/mb/mbutils.c: 1730 - 1773

## Overview
Reports an error when a valid character in the source encoding cannot be represented in the destination encoding during character set conversion.

## Definition
```c
void report_untranslatable_char(int src_encoding, int dest_encoding, const char *mbstr, int len)
```

## Detailed Description
This function generates an error message when a character conversion operation encounters a character that exists in the source encoding but has no equivalent representation in the destination encoding. Unlike report_invalid_encoding which handles malformed byte sequences, this function deals with valid characters that simply cannot be translated between encodings.

The function determines the length of the untranslatable character using pg_encoding_mblen_or_incomplete, formats the character's bytes as hexadecimal values, and reports a detailed error message including both encoding names. It uses defensive programming to handle potentially buggy conversion functions and limits the byte display to prevent buffer overruns.

## Parameters / Member Variables
- `src_encoding`: Integer identifier for the source character encoding
- `dest_encoding`: Integer identifier for the destination character encoding
- `mbstr`: Pointer to the start of the untranslatable character sequence
- `len`: Remaining length of the string from the untranslatable position (must be greater than zero)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_mblen_or_incomplete](../p/pg_encoding_mblen_or_incomplete.md) (determines character length safely)
  - Min (macro for minimum value calculation)
  - sprintf (standard C library function for string formatting)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - pg_enc2name_tbl (encoding name lookup table)
- Called from (representative examples):
  - [local2local](../l/local2local.md)
  - [mic2latin](../m/mic2latin.md)
  - [latin2mic_with_table](../l/latin2mic_with_table.md)
  - [mic2latin_with_table](../m/mic2latin_with_table.md)
  - [UtfToLocal](../U/UtfToLocal.md)
  - [LocalToUtf](../L/LocalToUtf.md)
  - Various encoding-specific conversion functions

## Notes and Other Information
- Uses ERRCODE_UNTRANSLATABLE_CHARACTER as the SQL error code, distinct from invalid encoding errors
- The function never returns - it always throws an error using ereport with ERROR level
- Formats untranslatable character bytes as hexadecimal values for identification
- Limits display to 8 bytes maximum to prevent buffer overflow
- Uses defensive programming with pg_encoding_mblen_or_incomplete to handle potentially buggy conversions
- Provides comprehensive error messages including source encoding, destination encoding, and the specific character bytes
- Specifically designed for valid characters that cannot be converted, not for invalid byte sequences
- The len parameter represents remaining string length, not the length of the untranslatable character
- Commonly used in PostgreSQL's character set conversion system when characters exist in one encoding but not another