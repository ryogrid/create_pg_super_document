# mic_to_win1251

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:387-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L387-L402)

## Overview
Converts text from PostgreSQL internal MULE encoding (MIC) to Windows Cyrillic encoding (WIN1251), handling Cyrillic character conversion through an intermediate KOI8-R encoding.

## Definition
```c
Datum mic_to_win1251(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL encoding conversion function that transforms text encoded in MULE Internal Code (MIC) format to Windows Cyrillic (WIN1251) encoding. The conversion process uses an intermediate KOI8-R encoding stage, leveraging the `mic2latin_with_table` function with a KOI8-R to WIN1251 conversion table (`koi2win1251`). This two-step conversion approach allows PostgreSQL to handle Cyrillic text conversion between different encoding systems efficiently.

The function follows PostgreSQL's standard conversion function interface, accepting source and destination buffers along with length and error handling parameters. It validates the encoding conversion arguments before performing the actual conversion.

## Parameters / Member Variables
- `src`: Source buffer containing text in MULE Internal Code (MIC) encoding
- `dest`: Destination buffer to store the converted WIN1251 encoded text
- `len`: Length of the input text to be converted
- `noError`: Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_ENCODING_CONVERSION_ARGS
  - [mic2latin_with_table](mic2latin_with_table.md)
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - PG_RETURN_INT32
- Constants used:
  - PG_MULE_INTERNAL
  - PG_WIN1251
  - LC_KOI8_R
  - koi2win1251 (conversion table)
- Called from:
  - No direct callers found (likely registered as encoding conversion function)

## Notes and Other Information
- Located in cyrillic_and_mic.c, indicating it's part of PostgreSQL's Cyrillic encoding conversion suite
- Uses a two-stage conversion process: MIC → KOI8-R → WIN1251
- Returns the number of converted characters as an integer
- Part of PostgreSQL's pluggable encoding conversion system
- The function is likely registered in the system catalogs as an encoding conversion function rather than being called directly

## Simplified Source

```c
Datum mic_to_win1251(PG_FUNCTION_ARGS) {
    // Extract conversion parameters from PostgreSQL function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate that we're converting from MULE internal to Windows-1251
    CHECK_ENCODING_CONVERSION_ARGS(PG_MULE_INTERNAL, PG_WIN1251);

    // Convert using koi2win1251 translation table to map KOI8-R to WIN1251 equivalents
    int converted = mic2latin_with_table(src, dest, len, LC_KOI8_R, PG_WIN1251, koi2win1251, noError);

    // Return number of bytes successfully converted
    PG_RETURN_INT32(converted);
}
```