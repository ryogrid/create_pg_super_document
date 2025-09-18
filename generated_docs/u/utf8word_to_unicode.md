# utf8word_to_unicode

## Location
src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c: 91 - 127

## Overview
Converts word-formatted UTF-8 representation back to Unicode code points, serving as the inverse operation to unicode_to_utf8word.

## Definition
```c
static inline uint32 utf8word_to_unicode(uint32 c)
```

## Detailed Description
The `utf8word_to_unicode` function performs the reverse transformation of `unicode_to_utf8word`, decoding UTF-8 encoded bytes packed in a 32-bit word format back to Unicode code points. The function implements the standard UTF-8 decoding algorithm, handling all four UTF-8 sequence lengths:

1. **1-byte sequence** (0x00-0x7F): ASCII characters passed through directly
2. **2-byte sequence** (0x80-0xFFFF): Extracts 11 bits from 110xxxxx 10xxxxxx pattern
3. **3-byte sequence** (0x10000-0xFFFFFF): Extracts 16 bits from 1110xxxx 10xxxxxx 10xxxxxx pattern
4. **4-byte sequence** (0x1000000-0xFFFFFFFF): Extracts 21 bits from 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx pattern

The function uses bit shifting and masking operations to extract the payload bits from each UTF-8 byte, then combines them using bitwise OR operations to reconstruct the original Unicode code point.

## Parameters / Member Variables
- `c`: A 32-bit unsigned integer containing the word-formatted UTF-8 sequence to be decoded

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - conv_utf8_to_18030 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:157)

## Notes and Other Information
This function is the mathematical inverse of `unicode_to_utf8word` and ensures that `utf8word_to_unicode(unicode_to_utf8word(x)) == x` for any valid Unicode code point. The function is used in the UTF-8 to GB18030 conversion process within PostgreSQL's character encoding system. It assumes the input word contains valid UTF-8 sequences and does not perform extensive validation. The function is declared as `static inline` for performance optimization in character conversion operations.