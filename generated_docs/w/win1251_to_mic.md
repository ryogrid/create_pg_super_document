# win1251_to_mic

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:371-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L371-L386)

## Overview
Converts text from Windows-1251 (Cyrillic) encoding to PostgreSQL's internal MULE encoding format using a translation table.

## Definition
```c
Datum win1251_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from Windows-1251 (CP1251) encoding to MULE (Multi-Language Environment) internal encoding format. Windows-1251 is Microsoft's Cyrillic character encoding commonly used in Windows environments for Russian and other Cyrillic scripts. The function uses the win12512koi translation table to map Windows-1251 characters to their KOI8-R equivalents before converting to MULE format, delegating the actual conversion work to the generic latin2mic_with_table helper function.

## Parameters / Member Variables
The function follows PostgreSQL's standard function argument protocol (PG_FUNCTION_ARGS), which provides:
- `src` (argument 2): Source string in Windows-1251 encoding to be converted
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
  - `win12512koi` - Translation table mapping Windows-1251 characters to KOI8-R equivalents
  - `PG_RETURN_INT32` - Return integer result to PostgreSQL
- Called from:
  - PostgreSQL encoding conversion system (no direct callers found in codebase)

## Notes and Other Information
- Part of the cyrillic_and_mic conversion module located in src/backend/utils/mb/conversion_procs/
- Uses the LC_KOI8_R locale constant and PG_WIN1251 encoding identifier
- Returns the number of input bytes successfully converted
- The win12512koi table provides mapping for characters from 0x80-0xFF, translating Windows-1251 Cyrillic characters to their KOI8-R equivalents
- Handles the character encoding differences between Windows-1251 and the KOI8-R-based MULE internal representation
- Windows-1251 is widely used in Russian Windows systems and differs from both KOI8-R and ISO-8859-5 in its character mappings
- This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system rather than called directly

## Simplified Source

```c
Datum win1251_to_mic(PG_FUNCTION_ARGS) {
    // Extract conversion parameters from PostgreSQL function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate that we're converting from Windows-1251 to MULE internal
    CHECK_ENCODING_CONVERSION_ARGS(PG_WIN1251, PG_MULE_INTERNAL);

    // Convert using win12512koi translation table to map WIN1251 to KOI8-R equivalents
    int converted = latin2mic_with_table(src, dest, len, LC_KOI8_R, PG_WIN1251, win12512koi, noError);

    // Return number of bytes successfully converted
    PG_RETURN_INT32(converted);
}
```