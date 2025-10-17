# iso_to_koi8r

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:531-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L531-L546)

## Overview
Converts a string from ISO-8859-5 (Cyrillic) encoding to KOI8-R (Russian Cyrillic) encoding using PostgreSQL's character conversion framework.

## Definition

```c
Datum
iso_to_koi8r(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs character encoding conversion from ISO-8859-5 to KOI8-R using a predefined character mapping table. ISO-8859-5 is part of the ISO-8859 series of ASCII-compatible character encodings that provides Latin/Cyrillic characters, while KOI8-R (Kod Obmena Informatsiey 8-bit Russian) is a character encoding designed specifically for Russian and other Cyrillic alphabets.

The function utilizes the local2local conversion utility function with the iso2koi mapping table to perform character-by-character transformation, enabling conversion from the ISO standard Cyrillic encoding to the Russian-specific KOI8-R encoding.

## Parameters / Member Variables
- : Source string in ISO-8859-5 encoding (null-terminated C string)
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
  - iso2koi (character mapping table at line 66)
  - PG_ISO_8859_5, PG_KOI8R (encoding constants)
  - PG_RETURN_INT32 (return value macro)
- Called from:
  - PostgreSQL's encoding conversion system (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:531-546
- Uses a static lookup table (iso2koi) containing 256 byte mappings for character conversion
- Enables conversion between international ISO standard and Russian-specific Cyrillic encodings
- Part of PostgreSQL's comprehensive international character encoding support
- Returns the number of bytes converted, allowing detection of conversion errors or partial conversions
- Facilitates data exchange between systems using different Cyrillic encoding standards

## Simplified Source

```c
Datum iso_to_koi8r(PG_FUNCTION_ARGS) {
    // Extract function parameters
    unsigned char *src = PG_GETARG_CSTRING(2);   // Source ISO-8859-5 string
    unsigned char *dest = PG_GETARG_CSTRING(3);  // Destination KOI8-R buffer
    int len = PG_GETARG_INT32(4);                 // Length to convert
    bool noError = PG_GETARG_BOOL(5);            // Error handling flag

    // Validate encoding compatibility
    CHECK_ENCODING_CONVERSION_ARGS(PG_ISO_8859_5, PG_KOI8R);

    // Perform ISO to KOI8-R Cyrillic conversion using mapping table
    int converted = local2local(src, dest, len, PG_ISO_8859_5, PG_KOI8R, iso2koi, noError);

    return converted;  // Return number of bytes converted
}
```