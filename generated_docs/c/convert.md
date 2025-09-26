# convert

## Location
[src/timezone/zic.c:1992-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1992-L2002)

## Overview
Converts a 32-bit integer value to big-endian byte order for storage in timezone database files.

## Definition

```c
static void
convert(const int32 val, char *const buf)
```
## Detailed Description
The  function is a utility function in PostgreSQL's timezone compiler () that converts a 32-bit integer value into big-endian (most significant byte first) byte order and stores it in a provided buffer. This function is essential for creating portable timezone database files that can be read consistently across different computer architectures.

The function uses bit shifting operations to extract each byte of the 32-bit integer, starting from the most significant byte (bits 24-31) and working down to the least significant byte (bits 0-7). Each byte is stored sequentially in the output buffer, resulting in a big-endian representation.

This endian conversion ensures that timezone database files created by  are portable across different computer architectures, regardless of the host system's native endianness.

## Parameters / Member Variables
- : The 32-bit integer value to convert to big-endian byte order
- : Output buffer to store the 4-byte big-endian representation (must be at least 4 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only basic C operations)
- Called from (representative examples):
  - [puttzcode](../p/puttzcode.md) (to write timezone data values)
  - DO macro (multiple times for writing timezone file data structures)

## Notes and Other Information
- This function ensures portability of timezone database files across different computer architectures
- The output is always in big-endian format regardless of the host system's native endianness
- The function assumes the output buffer has at least 4 bytes of available space
- Used extensively throughout the timezone file writing process to ensure consistent binary format
- Part of the timezone database binary format specification that requires big-endian integer storage
- The bit shifting approach is efficient and works correctly on both big-endian and little-endian systems