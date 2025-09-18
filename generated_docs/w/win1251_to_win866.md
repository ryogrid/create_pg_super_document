# win1251_to_win866

## Location
src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c: 515 - 530

## Overview
Converts a string from WIN1251 (Windows Cyrillic) encoding to WIN866 (IBM/MS-DOS Cyrillic) encoding using PostgreSQL's character conversion framework.

## Definition


## Detailed Description
This function performs character encoding conversion from WIN1251 to WIN866, providing the reverse conversion of win866_to_win1251. It converts from WIN1251 (the Windows Cyrillic code page) to WIN866 (the MS-DOS/IBM PC Cyrillic code page) using a predefined character mapping table. The function utilizes the local2local conversion utility function with the win12512win866 mapping table.

This conversion enables data transfer from modern Windows environments to legacy DOS systems or applications that require DOS Cyrillic encoding.

## Parameters / Member Variables
- : Source string in WIN1251 encoding (null-terminated C string)
- : Destination buffer for WIN866 encoded string (null-terminated C string)
- : Length of the source string in bytes
- : Error handling flag - if true, conversion continues on invalid characters; if false, throws error on conversion failures

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (parameter extraction macro)
  - PG_GETARG_INT32 (parameter extraction macro)
  - PG_GETARG_BOOL (parameter extraction macro)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [local2local](../l/local2local.md) (core conversion function in src/backend/utils/mb/conv.c:33)
  - win12512win866 (character mapping table at line 206)
  - PG_WIN1251, PG_WIN866 (encoding constants)
  - PG_RETURN_INT32 (return value macro)
- Called from:
  - PostgreSQL's encoding conversion system (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:515-530
- Uses a static lookup table (win12512win866) containing 256 byte mappings for reverse character conversion
- Counterpart function to win866_to_win1251, providing bidirectional conversion capability
- Enables backward compatibility for legacy DOS applications
- Part of PostgreSQL's comprehensive Windows/DOS Cyrillic encoding support
- Returns the number of bytes converted for validation and partial conversion detection