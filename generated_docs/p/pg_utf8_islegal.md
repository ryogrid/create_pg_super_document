# pg_utf8_islegal

## Location
[src/common/wchar.c:1989-2050](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1989-L2050)

## Overview
Validates whether a UTF-8 encoded character sequence is legal according to RFC 3629, ensuring proper encoding without security vulnerabilities.

## Definition

```c
bool
pg_utf8_islegal(const unsigned char *source, int length)
```
## Detailed Description
This function implements a comprehensive validator for UTF-8 character sequences that directly follows the rules specified in RFC 3629. It performs strict validation to prevent security hazards by ensuring that each Unicode character has exactly one valid encoding representation. The function specifically guards against overlong encodings, where a character that could be represented in fewer bytes is artificially encoded using more bytes with high-order zero bits.

The validation process includes checking continuation byte ranges (0x80-0xBF) and applying special restrictions on the second byte for specific lead bytes to prevent overlong sequences and invalid Unicode ranges. For example, it prevents encoding of UTF-16 surrogate pairs (0xED lead byte) and ensures 4-byte sequences don't exceed the maximum Unicode code point.

## Parameters / Member Variables
- : Pointer to the UTF-8 byte sequence to validate
- : Number of bytes in the sequence (expected to be obtained from pg_utf_mblen())

## Dependencies
- Functions called/Symbols referenced:
  - (None - implements validation logic directly)
- Called from (representative examples):
  - [chr](../c/chr.md) (Oracle compatibility function)
  - [UtfToLocal](../U/UtfToLocal.md) (character encoding conversion)
  - [utf8_to_iso8859_1](../u/utf8_to_iso8859_1.md) (character set conversion)
  - [pg_utf8_string_len](pg_utf8_string_len.md) (SASL string processing)
  - [pg_utf8_verifychar](pg_utf8_verifychar.md) (character verification)

## Notes and Other Information
- Assumes length parameter has been validated by pg_utf_mblen() and that sufficient bytes are available in the buffer
- Rejects UTF-8 sequences of 5 and 6 bytes, limiting support to 4-byte sequences maximum
- Uses fall-through switch statements for efficient validation of multi-byte sequences
- Special validation rules for specific lead bytes:
  - 0xE0: Second byte must be 0xA0-0xBF (prevents overlong 3-byte sequences)
  - 0xED: Second byte must be 0x80-0x9F (prevents UTF-16 surrogate encoding)
  - 0xF0: Second byte must be 0x90-0xBF (prevents overlong 4-byte sequences)
  - 0xF4: Second byte must be 0x80-0x8F (prevents code points > U+10FFFF)
- Returns false for any byte in range 0x80-0xC1 as first byte (invalid or overlong)

## Simplified Source

```c
// Simplified version of pg_utf8_islegal
bool pg_utf8_islegal(const unsigned char *source, int length) {
    unsigned char byte;

    switch (length) {
        default:
            // Reject 5 and 6 byte sequences (not supported)
            return false;

        case 4:
            // Check 4th byte: must be continuation byte (0x80-0xBF)
            byte = source[3];
            if (byte < 0x80 || byte > 0xBF)
                return false;
            /* FALL THRU */

        case 3:
            // Check 3rd byte: must be continuation byte (0x80-0xBF)
            byte = source[2];
            if (byte < 0x80 || byte > 0xBF)
                return false;
            /* FALL THRU */

        case 2:
            // Check 2nd byte with special rules based on first byte
            byte = source[1];
            switch (*source) {
                case 0xE0:  // 3-byte sequence starting with 0xE0
                    if (byte < 0xA0 || byte > 0xBF)  // Prevent overlong encoding
                        return false;
                    break;
                case 0xED:  // 3-byte sequence starting with 0xED
                    if (byte < 0x80 || byte > 0x9F)  // Prevent surrogate encoding
                        return false;
                    break;
                case 0xF0:  // 4-byte sequence starting with 0xF0
                    if (byte < 0x90 || byte > 0xBF)  // Prevent overlong encoding
                        return false;
                    break;
                case 0xF4:  // 4-byte sequence starting with 0xF4
                    if (byte < 0x80 || byte > 0x8F)  // Prevent code points > U+10FFFF
                        return false;
                    break;
                default:    // Other multi-byte sequences
                    if (byte < 0x80 || byte > 0xBF)  // Must be continuation byte
                        return false;
                    break;
            }
            /* FALL THRU */

        case 1:
            // Check first byte
            byte = *source;
            if (byte >= 0x80 && byte < 0xC2)  // Invalid range (overlong or continuation)
                return false;
            if (byte > 0xF4)  // Beyond valid UTF-8 range
                return false;
            break;
    }

    return true;
}
```

Key simplifications made:
- Added comprehensive comments explaining each validation step
- Clarified the purpose of special byte range checks
- Explained overlong encoding prevention and security implications
- Maintained the efficient fall-through switch structure
- Preserved all RFC 3629 compliance checks
- Used more descriptive variable name (byte instead of a)