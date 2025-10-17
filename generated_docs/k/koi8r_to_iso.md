# koi8r_to_iso

## Location
[src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c:547-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c#L547-L562)

## Overview
Converts a string from KOI8-R (Cyrillic) encoding to ISO-8859-5 encoding using a character conversion table.

## Definition

```c
Datum
koi8r_to_iso(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs character encoding conversion from KOI8-R (a Russian/Cyrillic character encoding) to ISO-8859-5 (Latin/Cyrillic encoding). It uses a lookup table  to map characters from the high bit range (128-255) between the two encodings. The function leverages the generic  conversion mechanism that handles single-byte charset conversions between ASCII-superset encodings.

The conversion process validates the encoding arguments, performs the character-by-character translation using the conversion table, and returns the number of successfully converted bytes.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 2): Source string in KOI8-R encoding (unsigned char*)
  -  (arg 3): Destination buffer for ISO-8859-5 output (unsigned char*)
  -  (arg 4): Length of source string (int)
  -  (arg 5): If true, don't throw error on conversion failure (bool)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string arguments
  -  - Extract integer argument
  -  - Extract boolean argument
  -  - Validate encoding parameters
  -  - Generic single-byte charset conversion function
  -  - Return integer result
  -  - Static conversion table (128 bytes)
- Called from:
  - PostgreSQL encoding conversion system (via function registry)

## Notes and Other Information
- Located in 
- Part of PostgreSQL's multi-byte character support system
- Uses a 128-byte lookup table starting from character code 128 (0x80)
- ASCII characters (0-127) are copied directly without conversion
- The conversion table handles the mapping between KOI8-R and ISO-8859-5 Cyrillic character sets
- Returns the number of input bytes successfully processed
- Registered as PG_FUNCTION_INFO_V1 for PostgreSQL function call interface

## Simplified Source

```c
Datum koi8r_to_iso(PG_FUNCTION_ARGS) {
    // Extract function parameters
    unsigned char *src = PG_GETARG_CSTRING(2);   // Source KOI8-R string
    unsigned char *dest = PG_GETARG_CSTRING(3);  // Destination ISO-8859-5 buffer
    int len = PG_GETARG_INT32(4);                 // Length to convert
    bool noError = PG_GETARG_BOOL(5);            // Error handling flag

    // Validate encoding compatibility
    CHECK_ENCODING_CONVERSION_ARGS(PG_KOI8R, PG_ISO_8859_5);

    // Perform KOI8-R to ISO-8859-5 Cyrillic conversion using mapping table
    int converted = local2local(src, dest, len, PG_KOI8R, PG_ISO_8859_5, koi2iso, noError);

    return converted;  // Return number of bytes converted
}
```