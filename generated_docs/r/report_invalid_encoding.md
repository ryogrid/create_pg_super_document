# report_invalid_encoding

## Location
[src/backend/utils/mb/mbutils.c:1698-1729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1698-L1729)

## Overview
Reports an error when invalid multibyte character sequences are encountered, providing detailed information about the invalid bytes.

## Definition
```c
void report_invalid_encoding(int encoding, const char *mbstr, int len)
```

## Detailed Description
This function generates a descriptive error message when invalid byte sequences are detected in multibyte character strings. It analyzes the invalid sequence, formats the problematic bytes as hexadecimal values, and reports the error using PostgreSQL's error reporting system. The function determines the length of the invalid character sequence and formats up to 8 bytes for display in the error message.

The function uses pg_encoding_mblen_or_incomplete to determine how many bytes constitute the invalid character, then formats these bytes as space-separated hexadecimal values in a buffer. It limits the display to 8 bytes to prevent buffer overrun and provides a clear error message indicating the encoding name and the specific invalid byte sequence.

## Parameters / Member Variables
- `encoding`: Integer identifier for the character encoding being validated
- `mbstr`: Pointer to the start of the invalid byte sequence
- `len`: Remaining length of the string from the invalid position (must be greater than zero)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_mblen_or_incomplete](../p/pg_encoding_mblen_or_incomplete.md) (determines character or incomplete sequence length)
  - Min (macro for minimum value calculation)
  - sprintf (standard C library function for string formatting)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - pg_enc2name_tbl (encoding name lookup table)
- Called from (representative examples):
  - [CopyConversionError](../C/CopyConversionError.md)
  - [local2local](../l/local2local.md)
  - [latin2mic](../l/latin2mic.md)
  - [mic2latin](../m/mic2latin.md)
  - Various encoding conversion functions
  - [UtfToLocal](../U/UtfToLocal.md)
  - [LocalToUtf](../L/LocalToUtf.md)
  - [pg_verify_mbstr](../p/pg_verify_mbstr.md)
  - [pg_verify_mbstr_len](../p/pg_verify_mbstr_len.md)

## Notes and Other Information
- The function never returns - it always throws an error using ereport with ERROR level
- Formats invalid bytes as hexadecimal values (e.g., "0x41 0x42") for clear identification
- Limits display to 8 bytes maximum to prevent buffer overflow
- Uses ERRCODE_CHARACTER_NOT_IN_REPERTOIRE as the SQL error code
- The len parameter represents remaining string length, not the length of the invalid character
- Widely used throughout PostgreSQL's encoding conversion system for consistent error reporting
- Provides both the encoding name and specific byte values in error messages for debugging