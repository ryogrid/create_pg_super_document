# win_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c:81-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c#L81-L116)

## Overview
A PostgreSQL function that converts text from various Windows character encodings to UTF-8.

## Definition
```c
Datum win_to_utf8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `win_to_utf8` function is a PostgreSQL conversion procedure that converts text from Windows character encodings (such as WIN1250, WIN1251, WIN1252, etc.) to UTF-8 encoding. It supports 11 different Windows code pages including WIN866, WIN874, and WIN1250-WIN1258. The function uses pre-compiled mapping tables to perform efficient character conversion through radix tree lookups.

The function validates the encoding conversion arguments and iterates through a static mapping table to find the appropriate conversion map for the source encoding. Once found, it delegates the actual conversion to the `LocalToUtf` function.

## Parameters / Member Variables
- `encoding` (arg 0): Integer ID of the source Windows encoding 
- `arg 1`: Destination encoding ID (validated to be UTF-8)
- `src` (arg 2): Pointer to the source string to convert
- `dest` (arg 3): Pointer to the destination buffer for converted text
- `len` (arg 4): Length of the source string in bytes
- `noError` (arg 5): Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
  - PG_GETARG_CSTRING 
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [LocalToUtf](../L/LocalToUtf.md)
  - lengthof
  - ereport/ERROR
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c:81-116
- Part of PostgreSQL multibyte character conversion system
- Supports Windows code pages: 866, 874, 1250-1258
- Uses static mapping tables defined in separate .map files
- Returns the number of bytes successfully converted
- Throws an error if an unsupported Windows encoding ID is provided
- Function is registered with PostgreSQL via PG_FUNCTION_INFO_V1 macro