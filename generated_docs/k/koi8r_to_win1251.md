# koi8r_to_win1251

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:435-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L435-L450)

## Overview
Converts text directly from KOI8-R (Russian) encoding to Windows Cyrillic encoding (WIN1251), providing direct character mapping between these two Cyrillic encoding systems.

## Definition
```c
Datum koi8r_to_win1251(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL encoding conversion function that transforms text encoded in KOI8-R format directly to Windows Cyrillic (WIN1251) encoding. Unlike other conversion functions in this module that use intermediate stages through MIC encoding, this function performs a direct conversion using the `local2local` function with a KOI8-R to WIN1251 conversion table (`koi2win1251`).

KOI8-R is a widely used Cyrillic encoding that was popular in Unix and internet environments, while WIN1251 is the Windows standard for Cyrillic text. This direct conversion path provides efficient transformation between these two common Cyrillic encodings without requiring intermediate conversion steps.

## Parameters / Member Variables
- `src`: Source buffer containing text in KOI8-R encoding
- `dest`: Destination buffer to store the converted WIN1251 encoded text
- `len`: Length of the input text to be converted
- `noError`: Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_ENCODING_CONVERSION_ARGS
  - [local2local](../l/local2local.md)
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - PG_RETURN_INT32
- Constants used:
  - PG_KOI8R
  - PG_WIN1251
  - koi2win1251 (conversion table)
- Called from:
  - No direct callers found (likely registered as encoding conversion function)

## Notes and Other Information
- Located in cyrillic_and_mic.c, indicating it's part of PostgreSQL's Cyrillic encoding conversion suite
- Uses direct conversion approach: KOI8-R → WIN1251 (no intermediate encoding)
- Returns the number of converted characters as an integer
- Part of PostgreSQL's pluggable encoding conversion system
- The function is likely registered in the system catalogs as an encoding conversion function rather than being called directly
- More efficient than MIC-based conversions since it avoids intermediate conversion steps
- KOI8-R to WIN1251 is a common conversion need in Eastern European database systems