# pg_encoding_mblen_or_incomplete

## Location
[src/common/wchar.c:2147-2166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L2147-L2166)

## Overview
Safely returns the byte length of a multibyte character with buffer boundary checking, or INT_MAX if insufficient bytes remain to determine the length.

## Definition

```c
int
pg_encoding_mblen_or_incomplete(int encoding, const char *mbstr,
								size_t remaining)
```
## Detailed Description
This function provides a safer alternative to pg_encoding_mblen() by incorporating buffer boundary validation before attempting to determine character length. It specifically addresses scenarios where the input buffer may not contain sufficient bytes to safely determine a multibyte character's length, which is crucial when processing untrusted or incomplete input streams.

The function implements special handling for GB18030 encoding, which uniquely requires examining up to two bytes to determine character length. For GB18030 characters with the high bit set, the function ensures at least 2 bytes are available before proceeding. For all other encodings, it requires at least 1 byte. If insufficient bytes are available, the function returns INT_MAX as a sentinel value indicating an incomplete character.

The function serves as a critical safety mechanism in PostgreSQL's character processing pipeline, preventing buffer overruns when dealing with potentially truncated multibyte sequences.

## Parameters / Member Variables
- : PostgreSQL encoding identifier (e.g., PG_UTF8, PG_GB18030, etc.)
- : Pointer to the start of the multibyte character sequence
- : Number of bytes remaining in the buffer from the current position

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for testing high bit in byte)
  - PG_GB18030 (GB18030 encoding constant)
  - [pg_encoding_mblen](pg_encoding_mblen.md) (core length determination function)
- Called from (representative examples):
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting for invalid characters)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (error reporting for untranslatable characters)
  - [PQescapeStringInternal](../P/PQescapeStringInternal.md) (libpq string escaping with safety checks)
  - [PQescapeInternal](../P/PQescapeInternal.md) (libpq internal escaping routines)

## Notes and Other Information
- Returns INT_MAX as a sentinel value when insufficient bytes are available for length determination
- Special case handling for GB18030: requires 2 bytes for high-bit-set characters, 1 byte otherwise
- Treats zero remaining bytes as insufficient even for single-byte encodings, ensuring consistent behavior
- Essential for safe processing of incomplete or truncated multibyte character streams
- Used primarily in error handling and string escaping contexts where buffer safety is critical
- Complements pg_encoding_mblen() by adding buffer boundary protection
- The INT_MAX return value allows callers to distinguish between actual character lengths and incomplete sequences
- Performance consideration: adds minimal overhead over pg_encoding_mblen() while providing crucial safety