# utf_charcheck

## Location
[src/fe_utils/mbprint.c:82-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/mbprint.c#L82-L135)

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
  - [mb_utf_validate](../m/mb_utf_validate.md)

## Notes and Other Information
- Implements strict Unicode 3.1 validation rules
- Performs both structural UTF-8 validation and semantic Unicode validation
- Used as a building block for higher-level UTF-8 validation functions
- Returns exact byte count for valid sequences, enabling efficient string processing
- Critical for ensuring data integrity in PostgreSQL's UTF-8 text handling
- The validation includes checks for overlong encodings and reserved code points
- Assumes the input buffer contains enough bytes for the complete sequence

## Simplified Source

```c
static int
utf_charcheck(const unsigned char *c)
{
    // Single-byte ASCII character (0xxxxxxx)
    if ((*c & 0x80) == 0)
        return 1;

    // Two-byte character (110xxxxx 10xxxxxx)
    if ((*c & 0xe0) == 0xc0) {
        if ((c[1] & 0xc0) == 0x80 && (c[0] & 0x1f) > 0x01)
            return 2;
        return -1;
    }

    // Three-byte character (1110xxxx 10xxxxxx 10xxxxxx)
    if ((*c & 0xf0) == 0xe0) {
        if ((c[1] & 0xc0) == 0x80 && (c[2] & 0xc0) == 0x80) {
            // Check for valid range and non-characters
            int z = c[0] & 0x0f;
            int yx = ((c[1] & 0x3f) << 6) | (c[2] & 0x3f);

            // Reject surrogates and non-characters
            if ((z == 0x0d && (yx & 0xb00) == 0x800) ||  // surrogates
                (z == 0x0f && ((yx & 0xffe) == 0xffe ||
                              ((yx & 0xf80) == 0xd80))))  // non-characters
                return -1;
            return 3;
        }
        return -1;
    }

    // Four-byte character (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
    if ((*c & 0xf8) == 0xf0) {
        int u = ((c[0] & 0x07) << 2) | ((c[1] & 0x30) >> 4);

        if ((c[1] & 0xc0) == 0x80 && (c[2] & 0xc0) == 0x80 &&
            (c[3] & 0xc0) == 0x80 && u > 0 && u <= 0x10) {
            // Check for non-characters ending in FFFE/FFFF
            if ((c[1] & 0x0f) == 0x0f && (c[2] & 0x3f) == 0x3f &&
                (c[3] & 0x3e) == 0x3e)
                return -1;
            return 4;
        }
        return -1;
    }

    return -1;  // Invalid UTF-8 start byte
}
```