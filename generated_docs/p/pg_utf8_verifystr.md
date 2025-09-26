# pg_utf8_verifystr

## Location
[src/common/wchar.c:1891-1901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1891-L1901)

## Overview
Validates UTF-8 encoded string data by checking each byte sequence for conformance to UTF-8 encoding rules and returns the number of valid bytes found.

## Definition
static int pg_utf8_verifystr(const unsigned char *s, int len)

## Detailed Description
pg_utf8_verifystr is a static function that validates UTF-8 encoded string data using a combination of optimized vectorized processing for ASCII-only chunks and careful byte-by-byte validation for multibyte sequences. The function implements a fast-path optimization that can skip full UTF-8 validation for chunks that are entirely ASCII, while falling back to detailed validation when non-ASCII characters or invalid sequences are encountered.

The function uses a finite state machine approach with predefined states (BGN/END for beginning/end, ERR for error, and various continuation states) to track the validation progress through multibyte UTF-8 sequences. When the fast path encounters invalid data, it restarts from the beginning using the slow path to accurately count valid bytes.

The implementation is designed for high performance with vectorized operations where possible, using a stride of two vector widths to allow compiler loop unrolling while maintaining the ability to fall back to precise byte-wise validation when needed.

## Parameters / Member Variables
- : Pointer to the unsigned char array containing the UTF-8 data to be validated
- : Length in bytes of the data to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [is_valid_ascii](../i/is_valid_ascii.md) - Checks if a memory chunk contains only ASCII characters
  - [utf8_advance](../u/utf8_advance.md) - Advances the UTF-8 validation state machine through a chunk of data
  - [pg_utf8_verifychar](pg_utf8_verifychar.md) - Validates a single UTF-8 character sequence
  - [pg_utf_mblen](pg_utf_mblen.md) - Returns the byte length of a UTF-8 character
  - BGN - Begin/End state constant for the UTF-8 state machine
  - END - End state constant (same as BGN)
  - ERR - Error state constant
  - STRIDE_LENGTH - Macro defining vectorized processing chunk size

- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) - Used as part of encoding validation function table

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit (wchar.c)
- Uses vectorized processing with SIMD optimizations for performance on large ASCII-heavy strings
- Implements proper UTF-8 validation according to RFC 3629 rules
- Returns the count of valid bytes found, allowing callers to identify exactly where invalid sequences begin
- Part of PostgreSQL's character encoding infrastructure, specifically used in the encoding function table for UTF-8 validation
- The function handles incomplete multibyte sequences at chunk boundaries by backtracking to find proper sequence starts
- Includes an optimization where pure ASCII chunks can bypass full UTF-8 validation entirely