# utf8_to_iso8859_1

## Location
src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c: 74 - 139

## Overview
Converts text from UTF-8 encoding to ISO-8859-1 (Latin-1) encoding, handling the conversion from multi-byte UTF-8 sequences to single-byte Latin-1 characters.

## Definition
```c
Datum utf8_to_iso8859_1(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts a string from UTF-8 encoding to ISO-8859-1 (Latin-1) encoding. The conversion process includes:
- ASCII characters (0x00-0x7F) are copied directly as they are identical in both encodings
- UTF-8 multi-byte sequences are decoded and validated
- Only 2-byte UTF-8 sequences representing characters in the Latin-1 range (0x80-0xFF) can be converted
- Characters outside the Latin-1 range result in conversion errors unless noError mode is enabled
- Comprehensive validation ensures UTF-8 sequences are legal and within convertible range

The function follows PostgreSQL's encoding conversion framework and includes robust error handling for invalid sequences and untranslatable characters.

## Parameters / Member Variables
- `PG_GETARG_CSTRING(2)` (src): Source string in UTF-8 encoding (null-terminated C string)
- `PG_GETARG_CSTRING(3)` (dest): Destination buffer for ISO-8859-1 encoded string (null-terminated C string)
- `PG_GETARG_INT32(4)` (len): Length of the source string in bytes
- `PG_GETARG_BOOL(5)` (noError): If true, conversion stops on error rather than throwing an exception

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - CHECK_ENCODING_CONVERSION_ARGS
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - IS_HIGHBIT_SET
  - pg_utf_mblen
  - [pg_utf8_islegal](../p/pg_utf8_islegal.md)
  - [report_untranslatable_char](../r/report_untranslatable_char.md)
  - PG_RETURN_INT32
- Constants used:
  - PG_UTF8
  - PG_LATIN1
- Called from:
  - No direct references found (likely called through PostgreSQL's conversion framework)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c:74-139
- Returns the number of bytes successfully converted from the source
- Only UTF-8 sequences representing characters in the range 0x80-0xFF can be converted to Latin-1
- UTF-8 sequences longer than 2 bytes are considered untranslatable and trigger errors
- The function validates UTF-8 sequence legality using pg_utf8_islegal before attempting conversion
- Decoding of 2-byte UTF-8 uses bit manipulation: `((c & 0x1f) << 6) | (src[1] & 0x3f)`
- Part of PostgreSQL's comprehensive multibyte character conversion system
- Handles edge cases like null bytes and malformed UTF-8 sequences gracefully