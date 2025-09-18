# iso_to_win1251

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 563 - 578

## Overview
Converts a string from ISO-8859-5 (Latin/Cyrillic) encoding to WIN1251 (Windows Cyrillic) encoding using a character conversion table.

## Definition
```c
Datum iso_to_win1251(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs character encoding conversion from ISO-8859-5 (Latin/Cyrillic encoding standard) to WIN1251 (Windows-1251, a Windows Cyrillic character encoding). It uses a lookup table `iso2win1251` to map characters from the high bit range (128-255) between the two encodings. The function leverages the generic `local2local` conversion mechanism that handles single-byte charset conversions between ASCII-superset encodings.

The conversion process validates the encoding arguments, performs the character-by-character translation using the conversion table, and returns the number of successfully converted bytes.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `src` (arg 2): Source string in ISO-8859-5 encoding (unsigned char*)
  - `dest` (arg 3): Destination buffer for WIN1251 output (unsigned char*)
  - `len` (arg 4): Length of source string (int)
  - `noError` (arg 5): If true, don't throw error on conversion failure (bool)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - Extract string arguments
  - `PG_GETARG_INT32` - Extract integer argument
  - `PG_GETARG_BOOL` - Extract boolean argument
  - `CHECK_ENCODING_CONVERSION_ARGS` - Validate encoding parameters
  - `local2local` - Generic single-byte charset conversion function
  - `PG_RETURN_INT32` - Return integer result
  - `iso2win1251` - Static conversion table (128 bytes)
- Called from:
  - PostgreSQL encoding conversion system (via function registry)

## Notes and Other Information
- Located in `src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:563-578`
- Part of PostgreSQL's multi-byte character support system
- Uses a 128-byte lookup table starting from character code 128 (0x80)
- ASCII characters (0-127) are copied directly without conversion
- The conversion table handles the mapping between ISO-8859-5 and WIN1251 Cyrillic character sets
- Returns the number of input bytes successfully processed
- Registered as PG_FUNCTION_INFO_V1 for PostgreSQL function call interface