# iso_to_mic

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 339 - 354

## Overview
Converts text from ISO-8859-5 (Cyrillic) encoding to PostgreSQL's internal MULE encoding format using a translation table.

## Definition
```c
Datum iso_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from ISO-8859-5 (Latin/Cyrillic) encoding to MULE (Multi-Language Environment) internal encoding format. Unlike the simpler koi8r_to_mic function, this conversion uses a character mapping table (iso2koi) to translate ISO-8859-5 characters to their KOI8-R equivalents before converting to MULE format. The function delegates the conversion work to latin2mic_with_table, which uses the iso2koi lookup table to handle the character set differences between ISO-8859-5 and the KOI8-R-based MULE internal representation.

## Parameters / Member Variables
The function follows PostgreSQL's standard function argument protocol (PG_FUNCTION_ARGS), which provides:
- `src` (argument 2): Source string in ISO-8859-5 encoding to be converted
- `dest` (argument 3): Destination buffer where converted MULE-encoded string will be written
- `len` (argument 4): Length of the source string in bytes
- `noError` (argument 5): Boolean flag indicating whether to suppress error reporting on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - Extract string arguments from PostgreSQL function call
  - `PG_GETARG_INT32` - Extract integer argument from PostgreSQL function call  
  - `PG_GETARG_BOOL` - Extract boolean argument from PostgreSQL function call
  - `CHECK_ENCODING_CONVERSION_ARGS` - Validate encoding conversion parameters
  - [latin2mic_with_table](../l/latin2mic_with_table.md) - Perform the actual character conversion using a lookup table
  - `iso2koi` - Translation table mapping ISO-8859-5 characters to KOI8-R equivalents
  - `PG_RETURN_INT32` - Return integer result to PostgreSQL
- Called from:
  - PostgreSQL encoding conversion system (no direct callers found in codebase)

## Notes and Other Information
- Part of the cyrillic_and_mic conversion module located in src/backend/utils/mb/conversion_procs/
- Uses the LC_KOI8_R locale constant and PG_ISO_8859_5 encoding identifier
- Returns the number of input bytes successfully converted
- The iso2koi table provides mapping for characters from 0x80-0xFF, translating ISO-8859-5 Cyrillic characters to their KOI8-R equivalents
- More complex than direct conversions because ISO-8859-5 and KOI8-R use different character encodings for Cyrillic letters
- This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system rather than called directly