# win866_to_win1251

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 499 - 514

## Overview
Converts a string from WIN866 (IBM/MS-DOS Cyrillic) encoding to WIN1251 (Windows Cyrillic) encoding using PostgreSQL's character conversion framework.

## Definition


## Detailed Description
This function performs character encoding conversion between two Windows/DOS Cyrillic encodings using a predefined character mapping table. It converts from WIN866 (the MS-DOS/IBM PC Cyrillic code page) to WIN1251 (the Windows Cyrillic code page). The function utilizes the local2local conversion utility function with the win8662win1251 mapping table to perform the character-by-character transformation.

Both encodings represent Cyrillic characters but with different byte values - WIN866 is the legacy DOS code page while WIN1251 is the modern Windows code page for Cyrillic scripts.

## Parameters / Member Variables
- : Source string in WIN866 encoding (null-terminated C string)
- : Destination buffer for WIN1251 encoded string (null-terminated C string)
- : Length of the source string in bytes
- : Error handling flag - if true, conversion continues on invalid characters; if false, throws error on conversion failures

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (parameter extraction macro)
  - PG_GETARG_INT32 (parameter extraction macro)
  - PG_GETARG_BOOL (parameter extraction macro)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [local2local](../l/local2local.md) (core conversion function in src/backend/utils/mb/conv.c:33)
  - win8662win1251 (character mapping table at line 186)
  - PG_WIN866, PG_WIN1251 (encoding constants)
  - PG_RETURN_INT32 (return value macro)
- Called from:
  - PostgreSQL's encoding conversion system (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:499-514
- Uses a static lookup table (win8662win1251) containing 256 byte mappings for character conversion
- Enables conversion between legacy DOS and modern Windows Cyrillic encodings
- Part of PostgreSQL's comprehensive Cyrillic encoding support
- Returns the number of bytes converted for error checking and partial conversion detection
- Useful for data migration between different Windows/DOS environments