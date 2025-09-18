# mic_to_iso

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:355-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L355-L370)

## Overview
Converts text from PostgreSQL's internal MULE encoding format to ISO-8859-5 (Cyrillic) encoding using a translation table.

## Definition
```c
Datum mic_to_iso(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from MULE (Multi-Language Environment) internal encoding format to ISO-8859-5 (Latin/Cyrillic) encoding. It is the inverse operation of iso_to_mic, converting from PostgreSQL's internal representation back to the ISO-8859-5 character encoding. The function uses the koi2iso translation table to map KOI8-R-based MULE characters to their ISO-8859-5 equivalents, delegating the actual conversion work to the generic mic2latin_with_table helper function.

## Parameters / Member Variables
The function follows PostgreSQL's standard function argument protocol (PG_FUNCTION_ARGS), which provides:
- `src` (argument 2): Source string in MULE internal encoding to be converted
- `dest` (argument 3): Destination buffer where converted ISO-8859-5 string will be written
- `len` (argument 4): Length of the source string in bytes
- `noError` (argument 5): Boolean flag indicating whether to suppress error reporting on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - Extract string arguments from PostgreSQL function call
  - `PG_GETARG_INT32` - Extract integer argument from PostgreSQL function call  
  - `PG_GETARG_BOOL` - Extract boolean argument from PostgreSQL function call
  - `CHECK_ENCODING_CONVERSION_ARGS` - Validate encoding conversion parameters
  - [mic2latin_with_table](mic2latin_with_table.md) - Perform the actual character conversion using a lookup table
  - `koi2iso` - Translation table mapping KOI8-R characters to ISO-8859-5 equivalents
  - `PG_RETURN_INT32` - Return integer result to PostgreSQL
- Called from:
  - PostgreSQL encoding conversion system (no direct callers found in codebase)

## Notes and Other Information
- Part of the cyrillic_and_mic conversion module located in src/backend/utils/mb/conversion_procs/
- Uses the LC_KOI8_R locale constant and PG_ISO_8859_5 encoding identifier
- Returns the number of input bytes successfully converted
- The koi2iso table provides reverse mapping from KOI8-R to ISO-8859-5 for characters in the range 0x80-0xFF
- Complementary to iso_to_mic, providing bidirectional conversion capability between ISO-8859-5 and MULE internal format
- Handles the character encoding differences between the KOI8-R-based MULE representation and ISO-8859-5 Cyrillic characters
- This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system rather than called directly