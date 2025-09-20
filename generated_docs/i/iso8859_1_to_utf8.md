# iso8859_1_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c:38-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c#L38-L73)

## Overview
Converts text from ISO-8859-1 (Latin-1) encoding to UTF-8 encoding, handling character conversion between single-byte and multi-byte UTF-8 representations.

## Definition

```c
Datum
iso8859_1_to_utf8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function converts a string from ISO-8859-1 (Latin-1) encoding to UTF-8 encoding. It processes each character in the source string:
- ASCII characters (0x00-0x7F) are copied directly as they are identical in both encodings
- High-bit characters (0x80-0xFF) are converted to 2-byte UTF-8 sequences using the standard UTF-8 encoding algorithm
- The function includes error handling for null bytes and invalid sequences, with optional no-error mode

The conversion follows the PostgreSQL function calling conventions, taking parameters through PG_FUNCTION_ARGS and returning a Datum.

## Parameters / Member Variables
-  (src): Source string in ISO-8859-1 encoding (null-terminated C string)
-  (dest): Destination buffer for UTF-8 encoded string (null-terminated C string) 
-  (len): Length of the source string in bytes
-  (noError): If true, conversion stops on error rather than throwing an exception

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - PG_GETARG_CSTRING  
  - PG_GETARG_INT32
  - CHECK_ENCODING_CONVERSION_ARGS
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - IS_HIGHBIT_SET
  - PG_RETURN_INT32
- Constants used:
  - PG_UTF8
  - PG_LATIN1
  - HIGHBIT
- Called from:
  - No direct references found (likely called through PostgreSQL's conversion framework)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c:38-73
- Returns the number of bytes successfully converted from the source
- High-bit characters are encoded as 2-byte UTF-8 sequences using bit manipulation:  for the first byte and  for the second byte
- Function validates encoding arguments and handles null byte detection for security
- Part of PostgreSQL's multibyte character conversion system