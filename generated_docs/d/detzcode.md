# detzcode

## Location
[src/timezone/localtime.c:118-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L118-L143)

## Overview
Decodes a 4-byte big-endian signed integer from a byte array, handling two's-complement representation across different machine architectures.

## Definition

```c
static int32
detzcode(const char *const codep)
```
## Detailed Description
The `detzcode` function is a utility for parsing timezone data files, which store numeric values in big-endian format as 4-byte signed integers. The function reads 4 bytes from the input pointer and reconstructs the integer value, properly handling the sign bit and ensuring correct two's-complement representation even on machines that don't natively use two's-complement arithmetic.

The function carefully handles the sign extension and ensures that the minimum representable value is correctly handled to avoid overflow issues during negation operations.

## Parameters / Member Variables
- `codep`: Pointer to a 4-byte array containing the big-endian encoded signed integer

## Dependencies
- Functions called/Symbols referenced:
  - TWOS_COMPLEMENT (macro for architecture detection)
- Called from (representative examples):
  - [tzloadbody](../t/tzloadbody.md) (multiple calls at lines 249, 250, 253, 254, 255, 256, 295, 334, 353, 354)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- Handles both positive and negative numbers using two's-complement representation
- The function includes special logic to handle the most negative representable integer safely
- Used extensively in timezone file parsing to decode transition times, UTC offsets, and other numeric data
- The big-endian format requirement comes from the timezone file format specification
- Includes protection against overflow when negating the minimum representable value

## Simplified Source

```c
static int32
detzcode(const char *const codep)
{
    int32 result;
    int32 one = 1;
    int32 halfmaxval = one << (32 - 2);  // 2^30
    int32 maxval = halfmaxval - 1 + halfmaxval;  // Max positive value
    int32 minval = -1 - maxval;  // Min negative value

    // Read first byte (with sign bit masked)
    result = codep[0] & 0x7f;

    // Read remaining 3 bytes to build the number
    for (int i = 1; i < 4; ++i)
        result = (result << 8) | (codep[i] & 0xff);

    // Handle negative numbers (sign bit set in first byte)
    if (codep[0] & 0x80)
    {
        // Perform two's-complement negation safely
        // Special handling for edge case to avoid overflow
        result -= !TWOS_COMPLEMENT(int32) && result != 0;
        result += minval;
    }

    return result;
}
```