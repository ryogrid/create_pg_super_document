# mic_to_win866

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 419 - 434

## Overview
Converts text from PostgreSQL internal MULE encoding (MIC) to Windows Cyrillic encoding (WIN866), handling Cyrillic character conversion through an intermediate KOI8-R encoding.

## Definition
```c
Datum mic_to_win866(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL encoding conversion function that transforms text encoded in MULE Internal Code (MIC) format to Windows Cyrillic (WIN866) encoding. The conversion process uses an intermediate KOI8-R encoding stage, leveraging the `mic2latin_with_table` function with a KOI8-R to WIN866 conversion table (`koi2win866`). This two-step conversion approach allows PostgreSQL to handle Cyrillic text conversion between different encoding systems efficiently.

WIN866 (also known as CP866) is a DOS Cyrillic codepage that was widely used in Eastern European computing environments. The function follows PostgreSQL's standard conversion function interface, accepting source and destination buffers along with length and error handling parameters.

## Parameters / Member Variables
- `src`: Source buffer containing text in MULE Internal Code (MIC) encoding
- `dest`: Destination buffer to store the converted WIN866 encoded text
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
  - PG_WIN866
  - LC_KOI8_R
  - koi2win866 (conversion table)
- Called from:
  - No direct callers found (likely registered as encoding conversion function)

## Notes and Other Information
- Located in cyrillic_and_mic.c, indicating it's part of PostgreSQL's Cyrillic encoding conversion suite
- Uses a two-stage conversion process: MIC → KOI8-R → WIN866
- Returns the number of converted characters as an integer
- Part of PostgreSQL's pluggable encoding conversion system
- The function is likely registered in the system catalogs as an encoding conversion function rather than being called directly
- WIN866 is also known as CP866, a legacy DOS Cyrillic codepage still used in some Eastern European systems