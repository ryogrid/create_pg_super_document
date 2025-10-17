# win866_to_koi8r

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:483-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L483-L498)

## Overview
Converts a string from WIN866 (IBM/MS-DOS Cyrillic) encoding to KOI8-R (Russian Cyrillic) encoding using PostgreSQL's character conversion framework.

## Definition

```c
Datum
win866_to_koi8r(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs character encoding conversion from WIN866 to KOI8-R using a predefined character mapping table. It is the reverse conversion of koi8r_to_win866, utilizing the local2local conversion utility function with the win8662koi mapping table. The function validates encoding compatibility and returns the number of bytes successfully converted.

WIN866 is the Cyrillic code page used in MS-DOS and IBM PC systems in Russian environments, while KOI8-R (Kod Obmena Informatsiey 8-bit Russian) is a character encoding designed for Russian and other Cyrillic alphabets commonly used in Unix/Linux systems.

## Parameters / Member Variables
- : Source string in WIN866 encoding (null-terminated C string)
- : Destination buffer for KOI8-R encoded string (null-terminated C string)
- : Length of the source string in bytes
- : Error handling flag - if true, conversion continues on invalid characters; if false, throws error on conversion failures

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (parameter extraction macro)
  - PG_GETARG_INT32 (parameter extraction macro)
  - PG_GETARG_BOOL (parameter extraction macro)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [local2local](../l/local2local.md) (core conversion function in src/backend/utils/mb/conv.c:33)
  - win8662koi (character mapping table at line 146)
  - PG_WIN866, PG_KOI8R (encoding constants)
  - PG_RETURN_INT32 (return value macro)
- Called from:
  - PostgreSQL's encoding conversion system (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:483-498
- Uses a static lookup table (win8662koi) containing 256 byte mappings for reverse character conversion
- Counterpart function to koi8r_to_win866, providing bidirectional conversion capability
- Part of PostgreSQL's modular character encoding conversion system
- Returns the number of bytes converted, enabling detection of partial conversions
- The conversion uses direct character mapping rather than Unicode intermediate conversion

## Simplified Source

```c
Datum win866_to_koi8r(PG_FUNCTION_ARGS) {
    // Extract function parameters
    unsigned char *src = PG_GETARG_CSTRING(2);   // Source WIN866 string
    unsigned char *dest = PG_GETARG_CSTRING(3);  // Destination KOI8-R buffer
    int len = PG_GETARG_INT32(4);                 // Length to convert
    bool noError = PG_GETARG_BOOL(5);            // Error handling flag

    // Validate encoding compatibility
    CHECK_ENCODING_CONVERSION_ARGS(PG_WIN866, PG_KOI8R);

    // Perform character conversion using mapping table
    int converted = local2local(src, dest, len, PG_WIN866, PG_KOI8R, win8662koi, noError);

    return converted;  // Return number of bytes converted
}
```