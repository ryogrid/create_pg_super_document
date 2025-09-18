# utf8_to_win

## Location
src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c: 117 - 150

## Overview
A PostgreSQL function that converts text from UTF-8 encoding to various Windows character encodings.

## Definition
```c
Datum utf8_to_win(PG_FUNCTION_ARGS)
```

## Detailed Description
The `utf8_to_win` function is a PostgreSQL conversion procedure that converts text from UTF-8 encoding to Windows character encodings (such as WIN1250, WIN1251, WIN1252, etc.). It is the inverse operation of `win_to_utf8` and supports the same 11 different Windows code pages including WIN866, WIN874, and WIN1250-WIN1258. The function uses pre-compiled mapping tables to perform efficient character conversion through radix tree lookups.

The function validates the encoding conversion arguments and iterates through a static mapping table to find the appropriate conversion map for the destination encoding. Once found, it delegates the actual conversion to the `UtfToLocal` function using the second mapping table (map2) which contains the UTF-8 to Windows encoding mappings.

## Parameters / Member Variables
- `arg 0`: Source encoding ID (validated to be UTF-8)
- `encoding` (arg 1): Integer ID of the destination Windows encoding
- `src` (arg 2): Pointer to the source UTF-8 string to convert
- `dest` (arg 3): Pointer to the destination buffer for converted text
- `len` (arg 4): Length of the source string in bytes
- `noError` (arg 5): Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
  - PG_GETARG_CSTRING
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - UtfToLocal
  - lengthof
  - ereport/ERROR
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c:117-150
- Part of PostgreSQL multibyte character conversion system
- Complementary function to `win_to_utf8`
- Supports Windows code pages: 866, 874, 1250-1258
- Uses static mapping tables defined in separate .map files (specifically the map2 entries)
- Returns the number of bytes successfully converted
- Throws an error if an unsupported Windows encoding ID is provided
- Function is registered with PostgreSQL via PG_FUNCTION_INFO_V1 macro
- Uses the second map (map2) from the conversion mapping tables for UTF-8 to Windows encoding conversion