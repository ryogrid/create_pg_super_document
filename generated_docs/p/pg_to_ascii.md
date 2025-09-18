# pg_to_ascii

## Location
[src/backend/utils/adt/ascii.c:29-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ascii.c#L29-L37)

## Overview
A static function that converts text from various single-byte character encodings to ASCII by replacing non-ASCII characters with their ASCII equivalents or spaces.

## Definition


## Detailed Description
The `pg_to_ascii` function performs character encoding conversion from specific single-byte encodings to ASCII. It processes each byte in the input buffer and converts characters above ASCII range (128+) to their closest ASCII equivalents using predefined translation tables. The function supports ISO-8859-1 (Latin-1), ISO-8859-2 (Latin-2), ISO-8859-15 (Latin-9), and Windows-1250 encodings.

For characters in the ASCII range (0-127), they are copied directly. For characters in the 128-159 range (when applicable), they are replaced with spaces as they represent control characters. For characters in the upper range (160+ for Latin encodings, 128+ for Windows-1250), they are translated using encoding-specific lookup tables that provide ASCII approximations.

The conversion is done in-place, meaning the destination buffer can be the same as the source buffer, and the converted string will have the same length as the original.

## Parameters / Member Variables
- `src`: Pointer to the start of the source byte array to be converted
- `src_end`: Pointer to one byte past the end of the source array (end boundary)
- `dest`: Pointer to the destination buffer where ASCII-converted bytes will be written
- `enc`: Integer encoding identifier (PG_LATIN1, PG_LATIN2, PG_LATIN9, or PG_WIN1250)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - pg_encoding_to_char (for error message formatting)
- Called from (representative examples):
  - [encode_to_ascii](../e/encode_to_ascii.md) (src/backend/utils/adt/ascii.c:106)

## Notes and Other Information
- The function uses hardcoded translation tables for each supported encoding
- Unsupported encodings trigger an error with ERRCODE_FEATURE_NOT_SUPPORTED  
- The translation is lossy - many non-ASCII characters map to the same ASCII character
- Characters in the 128-159 range are replaced with spaces for most encodings
- The function is designed for use with the PostgreSQL to_ascii() SQL function
- Translation tables contain ASCII approximations (e.g., 'ü' -> 'u', 'ñ' -> 'n')
- The RANGE_128 and RANGE_160 constants define the starting points for character translation ranges