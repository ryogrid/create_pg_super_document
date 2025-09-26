# get_hex

## Location
[src/backend/utils/adt/encode.c:176-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L176-L189)

## Overview
Internal static inline utility function that converts a single hexadecimal character to its corresponding 4-bit binary value using a lookup table.

## Definition

```c
static inline bool
get_hex(const char *cp, char *out)
```
## Detailed Description
The `get_hex` function is a helper function for hexadecimal decoding operations. It takes a single character pointer and attempts to convert the character to its hexadecimal digit value (0-15). The function uses the `hexlookup` table to perform the conversion efficiently. It handles both uppercase and lowercase hex digits ('0'-'9', 'A'-'F', 'a'-'f') and returns a boolean indicating whether the conversion was successful.

The function is declared as `static inline` for optimal performance since it's called frequently during hex decoding operations and should be inlined by the compiler.

## Parameters / Member Variables
- `cp`: Pointer to the character to be converted to hex digit
- `out`: Pointer to output location where the converted hex value (0-15) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `hexlookup` - Static lookup table for character to hex digit conversion
- Called from (representative examples):
  - `[hex_decode_safe](../h/hex_decode_safe.md)` - Safe hex decoding function
  - [hex_decode](../h/hex_decode.md) (in ecpglib) - ECPG hex decoding functionality
  - [PQunescapeBytea](../P/PQunescapeBytea.md) - libpq bytea unescaping function

## Notes and Other Information
- Returns true if the character is a valid hex digit, false otherwise
- Uses ASCII range check (< 127) for efficiency before table lookup
- The `hexlookup` table contains -1 for invalid characters and 0-15 for valid hex digits
- Handles both uppercase ('A'-'F') and lowercase ('a'-'f') hex digits
- The function is inlined for performance as it's called in tight loops during decoding
- Part of PostgreSQL's core hex decoding infrastructure
- Used extensively by both backend and interface library code