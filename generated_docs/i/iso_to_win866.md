# iso_to_win866

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:595-610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L595-L610)

## Overview
Converts a string from ISO-8859-5 (Latin/Cyrillic) encoding to WIN866 (DOS Cyrillic) encoding using a character conversion table.

## Definition
```c
Datum iso_to_win866(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs character encoding conversion from ISO-8859-5 (Latin/Cyrillic encoding standard) to WIN866 (also known as CP866 or DOS Cyrillic, a character encoding used in DOS systems for Russian and other Cyrillic scripts). It uses a lookup table `iso2win866` to map characters from the high bit range (128-255) between the two encodings. The function leverages the generic `local2local` conversion mechanism that handles single-byte charset conversions between ASCII-superset encodings.

The conversion process validates the encoding arguments, performs the character-by-character translation using the conversion table, and returns the number of successfully converted bytes.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `src` (arg 2): Source string in ISO-8859-5 encoding (unsigned char*)
  - `dest` (arg 3): Destination buffer for WIN866 output (unsigned char*)
  - `len` (arg 4): Length of source string (int)
  - `noError` (arg 5): If true, don't throw error on conversion failure (bool)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - Extract string arguments
  - `PG_GETARG_INT32` - Extract integer argument
  - `PG_GETARG_BOOL` - Extract boolean argument
  - `CHECK_ENCODING_CONVERSION_ARGS` - Validate encoding parameters
  - [local2local](../l/local2local.md) - Generic single-byte charset conversion function
  - `PG_RETURN_INT32` - Return integer result
  - `iso2win866` - Static conversion table (128 bytes)
- Called from:
  - PostgreSQL encoding conversion system (via function registry)

## Notes and Other Information
- Located in `src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:595-610`
- Part of PostgreSQL's multi-byte character support system
- Uses a 128-byte lookup table starting from character code 128 (0x80)
- ASCII characters (0-127) are copied directly without conversion
- The conversion table handles the mapping between ISO-8859-5 and WIN866 (CP866) Cyrillic character sets
- WIN866 is commonly used for DOS/legacy systems with Russian text
- Returns the number of input bytes successfully processed
- Registered as PG_FUNCTION_INFO_V1 for PostgreSQL function call interface