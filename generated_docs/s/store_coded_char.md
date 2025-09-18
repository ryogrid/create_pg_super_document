# store_coded_char

## Location
[src/backend/utils/mb/conv.c:353-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L353-L372)

## Overview
A static inline utility function that converts a 32-bit character code into a multibyte character sequence by storing only the significant bytes.

## Definition
```c
static inline unsigned char *store_coded_char(unsigned char *dest, uint32 code)
```

## Detailed Description
The `store_coded_char` function takes a 32-bit character representation and stores it as a multibyte sequence in the destination buffer. It efficiently packs the character by examining each byte of the 32-bit code and only storing the non-zero bytes, starting from the most significant byte. This allows for compact storage of characters that may use anywhere from 1 to 4 bytes depending on their Unicode code point value.

The function uses bit masking and shifting operations to extract individual bytes from the 32-bit code. It processes bytes in big-endian order (most significant byte first) and only stores bytes that contain actual data (non-zero values). This is particularly useful for Unicode character encodings where characters can have variable byte lengths.

## Parameters / Member Variables
- `dest`: Pointer to the destination buffer where the multibyte character sequence will be stored
- `code`: 32-bit character code to be converted and stored

## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic C operations)
- Called from (representative examples):
  - [UtfToLocal](../U/UtfToLocal.md) (multiple call sites for character conversion)
  - [LocalToUtf](../L/LocalToUtf.md) (multiple call sites for character conversion)

## Notes and Other Information
- This is a static inline function, optimized for performance with internal linkage
- Returns a pointer to the next byte position after the stored character sequence
- The function automatically handles variable-length character encodings by storing only significant bytes
- Uses big-endian byte ordering when storing the multibyte sequence
- Part of PostgreSQLs multibyte character encoding conversion subsystem
- Critical utility function used extensively in both UTF-8 to local and local to UTF-8 conversion routines