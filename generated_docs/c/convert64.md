# convert64

## Location
[src/timezone/zic.c:2003-2013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2003-L2013)

## Overview
A static utility function that converts a 64-bit integer value into its 8-byte big-endian binary representation for timezone data encoding.

## Definition

```c
struct attype *) avp)->at;
```
## Detailed Description
The  function takes a 64-bit integer value (of type ) and converts it into an 8-byte big-endian binary representation stored in the provided buffer. This function is part of PostgreSQL's timezone compilation utilities and is used specifically for encoding 64-bit values in timezone data files. The conversion uses bit shifting operations to extract each byte from the 64-bit value, starting from the most significant byte and working down to the least significant byte.

## Parameters / Member Variables
- : A 64-bit integer value of type  to be converted to binary representation
- : A character buffer (at least 8 bytes) where the big-endian binary representation will be stored

## Dependencies
- Functions called/Symbols referenced:
  - zic_t (type definition)
- Called from (representative examples):
  - [puttzcodepass](../p/puttzcodepass.md)

## Notes and Other Information
- The function uses a loop with bit shifting to convert the 64-bit value byte by byte
- The output is stored in big-endian format (most significant byte first)
- This function is static and only accessible within the zic.c compilation unit
- The shift operation starts at 56 bits and decrements by 8 for each byte position
- Part of the timezone data compilation infrastructure in PostgreSQL

## Simplified Source

```c
static void convert64(const zic_t val, char *const buf) {
    unsigned char *bytes = (unsigned char *) buf;

    // Convert 64-bit value to 8 bytes in big-endian format
    // Extract each byte from most significant (shift=56) to least significant (shift=0)
    for (int i = 0; i < 8; i++) {
        bytes[i] = val >> (56 - i * 8);
    }
}
```

**Key simplifications:**
- Clarified variable names (`bytes` instead of `b`)
- Added descriptive comments explaining the bit-shifting logic
- Simplified the shift calculation for better readability
- Preserved the essential big-endian conversion algorithm