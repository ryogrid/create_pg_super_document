# gb_unlinear

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:44-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c#L44-L59)

## Overview
Converts a linear code space representation back to 4-byte GB18030 encoded characters, serving as the inverse operation to gb_linear.

## Definition
```c
static inline uint32 gb_unlinear(uint32 lin)
```

## Detailed Description
The `gb_unlinear` function performs the reverse transformation of `gb_linear`, converting a linear numerical representation back to the original 4-byte GB18030 character encoding format. This function reconstructs the GB18030 byte structure by using division and modulo operations to extract the appropriate values for each byte position.

The function applies the inverse mathematical operations:
- Calculates each byte value by dividing the linear value by the appropriate divisor (12600, 1260, 10, 1)
- Uses modulo operations to extract the remainder for proper byte positioning
- Adds the base offset values (0x81 for first/third bytes, 0x30 for second/fourth bytes) to restore the original GB18030 byte ranges
- Packs the four bytes into a 32-bit value using bit shifting

## Parameters / Member Variables
- `lin`: A 32-bit unsigned integer representing the linear code space value to be converted back to GB18030 format

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - convutf8 (src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:161)

## Notes and Other Information
This function is the mathematical inverse of `gb_linear` and is essential for the UTF-8 to GB18030 conversion process. The function ensures that `gb_unlinear(gb_linear(x)) == x` for any valid 4-byte GB18030 character. Like `gb_linear`, it's declared as `static inline` for performance optimization in character encoding conversions. The bit shifting operations pack the bytes in big-endian format to match the expected GB18030 representation.