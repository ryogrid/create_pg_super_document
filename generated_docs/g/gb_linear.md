# gb_linear

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:32-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c#L32-L43)

## Overview
Converts 4-byte GB18030 encoded characters to a linear code space representation for efficient processing and conversion operations.

## Definition

```c
static inline uint32
gb_linear(uint32 gb)
```
## Detailed Description
The  function transforms 4-byte GB18030 character encodings into a linear numerical representation. GB18030 is a character encoding standard for Chinese text that uses a complex multi-byte structure. This function linearizes the encoding by extracting each byte and applying a mathematical formula that maps the GB18030 character space to a continuous linear space.

The function handles the specific byte range constraints of GB18030 4-byte sequences:
- First and third bytes: 0x81 to 0xfe (126 possible values)  
- Second and fourth bytes: 0x30 to 0x39 (10 possible values)

The linear conversion uses weighted multipliers (12600, 1260, 10, 1) that correspond to the positional significance of each byte, then subtracts a base offset to normalize the result starting from 0.

## Parameters / Member Variables
- `gb`: A 32-bit unsigned integer containing the 4-byte GB18030 encoded character, with bytes packed in big-endian format
## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - conv18030 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:132)
  - convutf8 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:161)

## Notes and Other Information
This function is part of PostgreSQL's character encoding conversion system, specifically for UTF-8 ↔ GB18030 conversions. The linear representation enables efficient mapping to Unicode code points and simplifies the conversion algorithms. The function is declared as  for performance optimization in character encoding operations.

## Simplified Source

```c
static inline uint32 gb_linear(uint32 gb) {
    // Extract each byte from the 4-byte GB18030 character
    uint32 b0 = (gb & 0xff000000) >> 24;  // First byte
    uint32 b1 = (gb & 0x00ff0000) >> 16;  // Second byte
    uint32 b2 = (gb & 0x0000ff00) >> 8;   // Third byte
    uint32 b3 = (gb & 0x000000ff);        // Fourth byte

    // Convert to linear space using weighted position values
    // GB18030 ranges: bytes 1,3: 0x81-0xfe (126 values), bytes 2,4: 0x30-0x39 (10 values)
    return b0 * 12600 + b1 * 1260 + b2 * 10 + b3 -
           (0x81 * 12600 + 0x30 * 1260 + 0x81 * 10 + 0x30);
}
```