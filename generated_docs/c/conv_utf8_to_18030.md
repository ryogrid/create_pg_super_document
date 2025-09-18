# conv_utf8_to_18030

## Location
src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c: 155 - 158

## Overview
A static helper function that converts UTF-8 encoded 4-byte sequences to GB18030 character encoding by mapping specific Unicode ranges to GB18030 linear code points.

## Definition
```c
static uint32 conv_utf8_to_18030(uint32 code)
```

## Detailed Description
This function performs the mapping of UTF-8 character codes to GB18030 encoding for characters that fall within specific Unicode ranges not covered by the standard GB18030 mapping tables. The function first converts the UTF-8 4-byte sequence to a Unicode code point, then checks if it falls within any of the predefined ranges that require special handling in GB18030.

The function uses a macro-based approach to efficiently check multiple Unicode ranges and convert them to corresponding GB18030 codes. For each range, it:
1. Checks if the Unicode code point falls within the specified range
2. If so, calculates the offset from the range minimum
3. Adds this offset to the corresponding GB18030 base code (converted to linear form)
4. Converts the result back to GB18030 4-byte format

The function handles 10 specific Unicode ranges:
- 0x0452-0x200F
- 0x2643-0x2E80  
- 0x361B-0x3917
- 0x3CE1-0x4055
- 0x4160-0x4336
- 0x44D7-0x464B
- 0x478E-0x4946
- 0x49B8-0x4C76
- 0x9FA6-0xD7FF
- 0xE865-0xF92B
- 0xFA2A-0xFE2F
- 0xFFE6-0xFFFF
- 0x10000-0x10FFFF

## Parameters / Member Variables
- `code`: A 32-bit unsigned integer representing a UTF-8 encoded character sequence (up to 4 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - utf8word_to_unicode: Converts UTF-8 4-byte sequence to Unicode code point
  - gb_linear: Converts GB18030 4-byte code to linear representation
  - gb_unlinear: Converts linear code back to GB18030 4-byte format
- Called from (representative examples):
  - utf8_to_gb18030: Main conversion function that uses this helper for special range mappings

## Notes and Other Information
- Returns 0 if no mapping exists for the given Unicode code point
- Uses a macro `convutf8` to reduce code duplication across multiple range checks
- This function handles characters that require 4-byte GB18030 encoding and are not in the basic multilingual plane
- The function is part of PostgreSQL's character encoding conversion system between UTF-8 and GB18030
- GB18030 is a Chinese national standard character encoding that extends GBK to cover all Unicode code points