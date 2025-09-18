# win1251_to_koi8r

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 451 - 466

## Overview
Converts text directly from Windows Cyrillic encoding (WIN1251) to KOI8-R (Russian) encoding, providing direct character mapping between these two Cyrillic encoding systems.

## Definition
```c
Datum win1251_to_koi8r(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL encoding conversion function that transforms text encoded in Windows Cyrillic (WIN1251) format directly to KOI8-R encoding. Like its counterpart `koi8r_to_win1251`, this function performs a direct conversion using the `local2local` function with a WIN1251 to KOI8-R conversion table (`win12512koi`), avoiding the need for intermediate conversion stages.

WIN1251 is the Windows standard for Cyrillic text, while KOI8-R is a widely used Cyrillic encoding that was popular in Unix and internet environments. This direct conversion path provides efficient bidirectional transformation capability between these two common Cyrillic encodings.

## Parameters / Member Variables
- `src`: Source buffer containing text in WIN1251 encoding
- `dest`: Destination buffer to store the converted KOI8-R encoded text
- `len`: Length of the input text to be converted
- `noError`: Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_ENCODING_CONVERSION_ARGS
  - local2local
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - PG_RETURN_INT32
- Constants used:
  - PG_WIN1251
  - PG_KOI8R
  - win12512koi (conversion table)
- Called from:
  - No direct callers found (likely registered as encoding conversion function)

## Notes and Other Information
- Located in cyrillic_and_mic.c, indicating it's part of PostgreSQL's Cyrillic encoding conversion suite
- Uses direct conversion approach: WIN1251 → KOI8-R (no intermediate encoding)
- Returns the number of converted characters as an integer
- Part of PostgreSQL's pluggable encoding conversion system
- The function is likely registered in the system catalogs as an encoding conversion function rather than being called directly
- Provides the reverse conversion capability to `koi8r_to_win1251`
- More efficient than MIC-based conversions since it avoids intermediate conversion steps
- Essential for data migration between Windows and Unix-based systems handling Cyrillic text