# local2local

## Location
[src/backend/utils/mb/conv.c:33-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L33-L88)

## Overview
A generic single byte charset encoding conversion function between two ASCII-superset encodings, facilitating character set transformations using lookup tables.

## Definition


## Detailed Description
The  function performs character set conversion between two single-byte ASCII-superset encodings using a translation table. It processes each byte in the source string, copying ASCII characters (0x00-0x7F) directly and converting high-bit characters (0x80-0xFF) using the provided lookup table. The function handles conversion errors by either reporting them or gracefully stopping conversion based on the  parameter. This is a fundamental building block for PostgreSQL's character encoding conversion system.

## Parameters / Member Variables
- : Pointer to the source string to be converted
- : Output buffer for the converted string (must be large enough to hold the result)
- : Length of the source string in bytes
- : PostgreSQL identifier for the source character encoding
- : PostgreSQL identifier for the target character encoding
- : Conversion lookup table starting from character 128 (0x80), where each entry contains the corresponding target charset code point or 0 if no equivalent exists
- : Boolean flag controlling error handling behavior - if true, conversion stops on error; if false, errors are reported

## Dependencies
- Functions called/Symbols referenced:
  - : Reports invalid character encoding errors
  - : Macro to check if the high bit (0x80) is set in a character
  - : Constant representing the high bit value (0x80)
  - : Reports characters that cannot be translated between encodings

- Called from (representative examples):
  - : Cyrillic encoding conversions
  - : Reverse Cyrillic encoding conversions
  - : Latin2 to Windows-1250 conversion
  - : Windows-1250 to Latin2 conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than the input length if  is true and an error occurs
- The output string is null-terminated
- ASCII characters (0x00-0x7F) are copied directly without translation
- Only high-bit characters (0x80-0xFF) undergo table-based conversion
- Used extensively in PostgreSQL's conversion procedures for various single-byte character encodings
- The function provides the foundation for many specific encoding conversion functions in the cyrillic_and_mic and latin2_and_win1250 conversion modules