# macaddr8_in

## Location
[src/backend/utils/adt/mac8.c:97-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L97-L233)

## Overview
A PostgreSQL input function that parses string representations of MAC addresses and converts them to the internal macaddr8 format, supporting both EUI-48 (6-byte) and EUI-64 (8-byte) MAC addresses.

## Definition
```c
Datum macaddr8_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the input parser for the macaddr8 PostgreSQL data type. It accepts MAC addresses in several common notations with different separators (colon ':', hyphen '-', or dot '.'). The function can handle both:

1. **EUI-48 (6-byte)**: Traditional 48-bit MAC addresses, which are automatically converted to EUI-64 format by inserting FF-FE in the middle
2. **EUI-64 (8-byte)**: Extended 64-bit MAC addresses used in IPv6 and other modern protocols

The function performs strict validation ensuring all separators are consistent throughout the address and that each byte is represented by exactly two hexadecimal digits. When a 6-byte MAC address is provided, it's converted to the 8-byte format by splitting the OUI and device identifier and inserting the standard FF-FE bytes in between.

## Parameters / Member Variables
- `str`: Input string containing the MAC address to parse (accessed via `PG_GETARG_CSTRING(0)`)
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [hex2_to_uchar](../h/hex2_to_uchar.md): Converts pairs of hex digits to bytes (called 8 times for each byte)
  - `[macaddr8](macaddr8.md)`: The target data structure for storing the parsed address
  - `PG_RETURN_MACADDR8_P`: PostgreSQL macro for returning macaddr8 values
  - `ereturn`: PostgreSQL error reporting function for invalid input
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocation function
- Called from:
  - PostgreSQL type system (automatically called for string-to-macaddr8 conversions)

## Notes and Other Information
- Accepts flexible input formats: "12:34:56:78:9A:BC:DE:F0", "12-34-56-78-9A-BC-DE-F0", "12.34.56.78.9A.BC.DE.F0"
- Automatically converts EUI-48 to EUI-64 by inserting FF-FE at the appropriate position
- Requires consistent separator usage throughout the entire address
- Supports leading and trailing whitespace
- Performs comprehensive input validation with detailed error messages
- The function is registered with PostgreSQL's type system and called automatically during input conversion
- Returns a Datum (PostgreSQL's generic return type) containing a pointer to the macaddr8 structure

## Simplified Source

```c
Datum macaddr8_in(PG_FUNCTION_ARGS) {
    const unsigned char *str = (unsigned char *) PG_GETARG_CSTRING(0);
    const unsigned char *ptr = str;
    bool badhex = false;
    macaddr8 *result;
    unsigned char bytes[8] = {0}; // a,b,c,d,e,f,g,h
    int count = 0;
    unsigned char spacer = '\0';

    // Skip leading whitespace
    while (*ptr && isspace(*ptr)) ptr++;

    // Parse hex byte pairs
    while (*ptr && *(ptr + 1) && count < 8) {
        count++;
        bytes[count-1] = hex2_to_uchar(ptr, &badhex);
        if (badhex) goto fail;

        ptr += 2; // Move past hex digits

        // Handle separators (:, -, .)
        if (*ptr == ':' || *ptr == '-' || *ptr == '.') {
            if (spacer == '\0') spacer = *ptr;
            else if (spacer != *ptr) goto fail;
            ptr++;
        }

        // Allow trailing whitespace after 6 or 8 bytes
        if ((count == 6 || count == 8) && isspace(*ptr)) {
            while (*++ptr && isspace(*ptr));
            if (*ptr) goto fail;
        }
    }

    // Convert EUI-48 to EUI-64 format
    if (count == 6) {
        bytes[7] = bytes[5]; // h = f
        bytes[6] = bytes[4]; // g = e
        bytes[5] = bytes[3]; // f = d
        bytes[3] = 0xFF;     // d = FF
        bytes[4] = 0xFE;     // e = FE
    } else if (count != 8) {
        goto fail;
    }

    // Create result structure
    result = (macaddr8 *) palloc0(sizeof(macaddr8));
    result->a = bytes[0]; result->b = bytes[1];
    result->c = bytes[2]; result->d = bytes[3];
    result->e = bytes[4]; result->f = bytes[5];
    result->g = bytes[6]; result->h = bytes[7];

    PG_RETURN_MACADDR8_P(result);

fail:
    ereturn(escontext, (Datum) 0,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"", "macaddr8", str)));
}
```