# is_utf16_surrogate_second

## Location
[src/include/mb/pg_wchar.h:547-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/mb/pg_wchar.h#L547-L552)

## Overview
Determines whether a given wide character value represents the second (low) surrogate in a UTF-16 surrogate pair.

## Definition
static inline bool is_utf16_surrogate_second(pg_wchar c)

## Detailed Description
This inline function checks if a PostgreSQL wide character value falls within the UTF-16 low surrogate range (0xDC00-0xDFFF). In UTF-16 encoding, Unicode code points above U+FFFF (supplementary characters) are encoded using surrogate pairs, where the low surrogate follows the high surrogate to complete the encoding of a single Unicode character.

The low surrogates occupy the range 0xDC00 to 0xDFFF and must always follow a corresponding high surrogate (0xD800-0xDBFF) to form a valid surrogate pair. This function is crucial for UTF-16 validation and processing in PostgreSQL's Unicode handling routines.

## Parameters / Member Variables
- `c`: The wide character value to test for being a UTF-16 low surrogate

## Dependencies
- Functions called/Symbols referenced: None (simple arithmetic comparison)
- Called from (representative examples):
  - [str_udeescape](../s/str_udeescape.md) (src/backend/parser/parser.c:432, 440, 471, 479)
  - [unistr](../u/unistr.md) (src/backend/utils/adt/varlena.c:6545, 6553, 6580, 6588, 6615, 6623)

## Notes and Other Information
- Defined as a static inline function for performance optimization
- Located in src/include/mb/pg_wchar.h with other Unicode utility functions
- The range 0xDC00-0xDFFF is reserved exclusively for low surrogates in the Unicode standard
- Must be preceded by a valid high surrogate to form a complete surrogate pair
- Used extensively in PostgreSQL's Unicode escape sequence processing and string functions
- Essential for proper validation and conversion of supplementary Unicode characters
- Invalid if encountered without a preceding high surrogate