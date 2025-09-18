# mic_to_koi8r

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 323 - 338

## Overview
Converts text from PostgreSQL's internal MULE encoding format to KOI8-R (Cyrillic) encoding.

## Definition
```c
Datum mic_to_koi8r(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from MULE (Multi-Language Environment) internal encoding format to KOI8-R encoding. It is the inverse operation of koi8r_to_mic, converting from PostgreSQL's internal representation back to the KOI8-R character encoding used in Russian computing environments. The function delegates the actual conversion work to the generic mic2latin helper function, which handles the reverse mapping from MIC codes to local character set codes.

## Parameters / Member Variables
The function follows PostgreSQL's standard function argument protocol (PG_FUNCTION_ARGS), which provides:
- `src` (argument 2): Source string in MULE internal encoding to be converted
- `dest` (argument 3): Destination buffer where converted KOI8-R string will be written
- `len` (argument 4): Length of the source string in bytes
- `noError` (argument 5): Boolean flag indicating whether to suppress error reporting on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - Extract string arguments from PostgreSQL function call
  - `PG_GETARG_INT32` - Extract integer argument from PostgreSQL function call  
  - `PG_GETARG_BOOL` - Extract boolean argument from PostgreSQL function call
  - `CHECK_ENCODING_CONVERSION_ARGS` - Validate encoding conversion parameters
  - [mic2latin](mic2latin.md) - Perform the actual character conversion
  - `PG_RETURN_INT32` - Return integer result to PostgreSQL
- Called from:
  - PostgreSQL encoding conversion system (no direct callers found in codebase)

## Notes and Other Information
- Part of the cyrillic_and_mic conversion module located in src/backend/utils/mb/conversion_procs/
- Uses the LC_KOI8_R locale constant and PG_KOI8R encoding identifier
- Returns the number of input bytes successfully converted
- The conversion handles multi-byte MULE sequences by extracting the actual character byte from 2-byte MULE sequences
- This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system rather than called directly
- Complementary to koi8r_to_mic, providing bidirectional conversion capability