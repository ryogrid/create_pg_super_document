# utf_charcheck

## Location
src/fe_utils/mbprint.c: 82 - 135

## Overview
A static function that validates UTF-8 character sequences according to Unicode 3.1 standards and returns the byte length of valid sequences or -1 for invalid ones.

## Definition
```c
static int utf_charcheck(const unsigned char *c)
```

## Detailed Description
This function performs comprehensive UTF-8 validation according to Unicode 3.1 compliance standards. It validates both the byte sequence structure and Unicode code point ranges. The function checks:

1. **Byte sequence validation**: Ensures proper UTF-8 encoding structure for 1-4 byte sequences
2. **Unicode range validation**: Rejects invalid Unicode code points including:
   - Code points > 0x10FFFF (beyond Unicode range)
   - Code points ending in 0xFFFE or 0xFFFF (non-characters)
   - Code points in range 0xFDD0-0xFDEF (non-characters)
   - Surrogate pairs (0xD800-0xDFFF range, invalid in UTF-8)

The function returns the number of bytes in the valid UTF-8 sequence (1-4) or -1 if the sequence is invalid.

## Parameters / Member Variables
- `c`: Pointer to the first byte of a UTF-8 character sequence to validate

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only bitwise operations and arithmetic)
- Called from (representative examples):
  - mb_utf_validate

## Notes and Other Information
- Implements strict Unicode 3.1 validation rules
- Performs both structural UTF-8 validation and semantic Unicode validation
- Used as a building block for higher-level UTF-8 validation functions
- Returns exact byte count for valid sequences, enabling efficient string processing
- Critical for ensuring data integrity in PostgreSQL's UTF-8 text handling
- The validation includes checks for overlong encodings and reserved code points
- Assumes the input buffer contains enough bytes for the complete sequence