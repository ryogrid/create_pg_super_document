# pg_encoding_set_invalid

## Location
[src/common/wchar.c:2051-2134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L2051-L2134)

## Overview
Creates a standardized 2-byte invalid character sequence for a given encoding that will be detected as invalid during verification.

## Definition

```c
void
pg_encoding_set_invalid(int encoding, char *dst)
```
## Detailed Description
This function fills a provided buffer with a carefully chosen 2-byte sequence that satisfies two critical properties: the sequence will report a multibyte length of 2 when passed to pg_encoding_mblen(), but will fail validation when passed to pg_encoding_verifymbstr(). This creates a reliable way to generate invalid character sequences for testing, error handling, and security-related functionality.

For UTF-8 encoding, it uses the byte sequence [0xC0, 0x20 (space)], which represents an overlong encoding that violates UTF-8 rules. For all other multibyte encodings, it uses the standardized invalid sequence [0x8D, 0x20 (space)]. This particular non-UTF8 sequence was specifically chosen through analysis to ensure it's rejected by all encoding verifiers while maintaining the required length properties.

The function includes an assertion that the target encoding supports multibyte characters (maximum length > 1), ensuring it's only used with appropriate encodings.

## Parameters / Member Variables
- : PostgreSQL encoding identifier (e.g., PG_UTF8, PG_EUC_JP, etc.)
- : Destination buffer to fill with the invalid sequence (must have space for at least 2 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_max_length](pg_encoding_max_length.md) (encoding capability check)
  - PG_UTF8 (UTF-8 encoding constant)
  - NONUTF8_INVALID_BYTE0 (0x8D - first byte of non-UTF8 invalid sequence)
  - NONUTF8_INVALID_BYTE1 (0x20 - second byte of invalid sequence)
- Called from (representative examples):
  - [fmtIdEnc](../f/fmtIdEnc.md) (identifier encoding formatting)
  - [appendStringLiteral](../a/appendStringLiteral.md) (string literal processing)
  - [PQescapeStringInternal](../P/PQescapeStringInternal.md) (libpq string escaping)
  - [test_enc_setup](../t/test_enc_setup.md) (regression testing)

## Notes and Other Information
- The invalid byte sequences were carefully selected to avoid conflicts with legitimate character encodings
- For historical reasons, some verifier implementations specifically reject the [0x8D, 0x20] pair, making it ideal for this purpose
- The space character (0x20) for the second byte maintains compatibility with historical PQescapeString() behavior
- This function is essential for security testing and ensuring robust handling of invalid character sequences
- The assertion prevents use with single-byte encodings where the concept of invalid multibyte sequences doesn't apply
- Used extensively in PostgreSQL's string processing and escaping functions to handle encoding validation