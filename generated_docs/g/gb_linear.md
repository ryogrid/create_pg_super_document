# gb_linear

## Location
src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c: 32 - 43

## Overview
Converts 4-byte GB18030 encoded characters to a linear code space representation for efficient processing and conversion operations.

## Definition


## Detailed Description
The  function transforms 4-byte GB18030 character encodings into a linear numerical representation. GB18030 is a character encoding standard for Chinese text that uses a complex multi-byte structure. This function linearizes the encoding by extracting each byte and applying a mathematical formula that maps the GB18030 character space to a continuous linear space.

The function handles the specific byte range constraints of GB18030 4-byte sequences:
- First and third bytes: 0x81 to 0xfe (126 possible values)  
- Second and fourth bytes: 0x30 to 0x39 (10 possible values)

The linear conversion uses weighted multipliers (12600, 1260, 10, 1) that correspond to the positional significance of each byte, then subtracts a base offset to normalize the result starting from 0.

## Parameters / Member Variables
- : A 32-bit unsigned integer containing the 4-byte GB18030 encoded character, with bytes packed in big-endian format

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - conv18030 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:132)
  - convutf8 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:161)

## Notes and Other Information
This function is part of PostgreSQL's character encoding conversion system, specifically for UTF-8 ↔ GB18030 conversions. The linear representation enables efficient mapping to Unicode code points and simplifies the conversion algorithms. The function is declared as  for performance optimization in character encoding operations.