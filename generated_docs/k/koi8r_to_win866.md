# koi8r_to_win866

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:467-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L467-L482)

## Overview
Converts a string from KOI8-R (Russian Cyrillic) encoding to WIN866 (IBM/MS-DOS Cyrillic) encoding using PostgreSQL's character conversion framework.

## Definition

```c
Datum
koi8r_to_win866(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs character encoding conversion from KOI8-R to WIN866 using a predefined character mapping table. It leverages the local2local conversion utility function to handle the actual character-by-character transformation. The function validates encoding compatibility and returns the number of bytes successfully converted.

KOI8-R (Kod Obmena Informatsiey 8-bit Russian) is a character encoding designed for Russian and other Cyrillic alphabets, while WIN866 is the Cyrillic code page used in MS-DOS and IBM PC systems in Russian environments.

## Parameters / Member Variables
- : Source string in KOI8-R encoding (null-terminated C string)
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
  - koi2win866 (character mapping table at line 166)
  - PG_KOI8R, PG_WIN866 (encoding constants)
  - PG_RETURN_INT32 (return value macro)
- Called from:
  - PostgreSQL's encoding conversion system (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:467-482
- Uses a static lookup table (koi2win866) containing 256 byte mappings for character conversion
- Part of PostgreSQL's modular character encoding conversion system
- Returns the number of bytes converted, which can be used to detect partial conversions
- The conversion is performed using a direct character mapping approach rather than Unicode intermediate conversion