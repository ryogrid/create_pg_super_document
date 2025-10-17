# win866_to_mic

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:403-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L403-L418)

## Overview
Converts text from Windows Cyrillic encoding (WIN866) to PostgreSQL internal MULE encoding (MIC), handling Cyrillic character conversion through an intermediate KOI8-R encoding.

## Definition
```c
Datum win866_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL encoding conversion function that transforms text encoded in Windows Cyrillic (WIN866) format to MULE Internal Code (MIC) encoding. The conversion process uses an intermediate KOI8-R encoding stage, leveraging the `latin2mic_with_table` function with a WIN866 to KOI8-R conversion table (`win8662koi`). This two-step conversion approach allows PostgreSQL to handle Cyrillic text conversion from Windows codepage 866 to the internal representation efficiently.

The function follows PostgreSQL's standard conversion function interface, accepting source and destination buffers along with length and error handling parameters. It validates the encoding conversion arguments before performing the actual conversion.

## Parameters / Member Variables
- `src`: Source buffer containing text in WIN866 encoding
- `dest`: Destination buffer to store the converted MULE Internal Code (MIC) encoded text
- `len`: Length of the input text to be converted
- `noError`: Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_ENCODING_CONVERSION_ARGS
  - [latin2mic_with_table](../l/latin2mic_with_table.md)
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - PG_RETURN_INT32
- Constants used:
  - PG_WIN866
  - PG_MULE_INTERNAL
  - LC_KOI8_R
  - win8662koi (conversion table)
- Called from:
  - No direct callers found (likely registered as encoding conversion function)

## Notes and Other Information
- Located in cyrillic_and_mic.c, indicating it's part of PostgreSQL's Cyrillic encoding conversion suite
- Uses a two-stage conversion process: WIN866 → KOI8-R → MIC
- Returns the number of converted characters as an integer
- Part of PostgreSQL's pluggable encoding conversion system
- The function is likely registered in the system catalogs as an encoding conversion function rather than being called directly
- WIN866 is also known as CP866, a DOS Cyrillic codepage commonly used in Eastern Europe

## Simplified Source

```c
Datum win866_to_mic(PG_FUNCTION_ARGS) {
    // Extract conversion parameters from PostgreSQL function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate that we're converting from WIN866 to MULE internal
    CHECK_ENCODING_CONVERSION_ARGS(PG_WIN866, PG_MULE_INTERNAL);

    // Convert using win8662koi translation table to map WIN866 to KOI8-R equivalents
    int converted = latin2mic_with_table(src, dest, len, LC_KOI8_R, PG_WIN866, win8662koi, noError);

    // Return number of bytes successfully converted
    PG_RETURN_INT32(converted);
}
```