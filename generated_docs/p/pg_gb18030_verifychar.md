# pg_gb18030_verifychar

## Location
src/common/wchar.c: 1641 - 1671

## Overview
Validates a single character in GB18030 encoding format, which is the official character encoding standard for Chinese text that supports variable-length characters (1, 2, or 4 bytes).

## Definition
static int pg_gb18030_verifychar(const unsigned char *s, int len)

## Detailed Description
This function performs character validation for GB18030 encoding, the official Chinese character encoding standard. GB18030 is a variable-length encoding that can represent characters using 1, 2, or 4 bytes. The function implements the complete validation logic for all three character formats: ASCII characters (1 byte), double-byte characters for common Chinese characters, and 4-byte characters for less common characters and Unicode compatibility. The validation ensures that each byte sequence follows the strict byte range requirements defined in the GB18030 specification.

## Parameters / Member Variables
- `s`: Pointer to the byte sequence to validate
- `len`: Length of the available buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
- Called from (representative examples):
  - [pg_gb18030_verifystr](pg_gb18030_verifystr.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the length of the validated character in bytes (1, 2, or 4) on success, or -1 on failure
- ASCII characters (0x00-0x7F) are handled as single bytes
- 2-byte sequences: first byte 0x81-0xFE, second byte 0x40-0x7E or 0x80-0xFE
- 4-byte sequences: first byte 0x81-0xFE, second byte 0x30-0x39, third byte 0x81-0xFE, fourth byte 0x30-0x39
- The 4-byte format is used for characters outside the basic multilingual plane and provides full Unicode compatibility
- GB18030 is mandatory for software products sold in China and is backward compatible with GB2312 and GBK encodings