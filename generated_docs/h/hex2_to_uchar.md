# hex2_to_uchar

## Location
[src/backend/utils/adt/mac8.c:59-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L59-L96)

## Overview
A static inline utility function that converts two consecutive hexadecimal digits to a single byte (unsigned char) value.

## Definition
```c
static inline unsigned char hex2_to_uchar(const unsigned char *ptr, bool *badhex)
```

## Detailed Description
This function parses two hexadecimal characters from a string and converts them into a single byte value. It uses a lookup table (`hexlookup`) to efficiently convert hexadecimal digits (0-9, A-F, a-f) to their numeric values. The function handles both uppercase and lowercase hex digits and provides error detection for invalid characters or end-of-string conditions.

The function performs validation on each character by checking if it's within the ASCII range (≤127) and uses the `hexlookup` table to determine if it's a valid hexadecimal digit. If any error is detected, it sets the `badhex` flag to true and returns 0.

## Parameters / Member Variables
- `ptr`: Pointer to the first of two consecutive hexadecimal characters to convert
- `badhex`: Pointer to a boolean flag that is set to true if invalid input is encountered

## Dependencies
- Functions called/Symbols referenced:
  - `hexlookup` (static lookup table for hex digit conversion)
- Called from (representative examples):
  - [macaddr8_in](../m/macaddr8_in.md) (multiple times for parsing MAC address components)
  - `lobits` macro (indirectly referenced)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the mac8.c compilation unit and will be inlined by the compiler for performance
- The function is specifically designed for parsing 8-byte MAC addresses (EUI-64 format)
- The `hexlookup` table maps ASCII values to hex digit values: '0'-'9' map to 0-9, 'A'-'F' and 'a'-'f' map to 10-15, all other values map to -1
- Error handling is done through the `badhex` parameter rather than throwing exceptions, following PostgreSQL's C-based error handling patterns

## Simplified Source

```c
static inline unsigned char hex2_to_uchar(const unsigned char *ptr, bool *badhex) {
    unsigned char result;
    signed char digit1, digit2;

    // Convert first hex digit
    if (*ptr > 127 || (digit1 = hexlookup[*ptr]) < 0) {
        *badhex = true;
        return 0;
    }
    result = digit1 << 4;  // Shift to upper nibble

    // Convert second hex digit
    ptr++;
    if (*ptr > 127 || (digit2 = hexlookup[*ptr]) < 0) {
        *badhex = true;
        return 0;
    }
    result += digit2;  // Add to lower nibble

    return result;
}
```