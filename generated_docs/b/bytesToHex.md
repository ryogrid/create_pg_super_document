# bytesToHex

## Location
[src/common/md5_common.c:28-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5_common.c#L28-L73)

## Overview
Converts a 16-byte array to its hexadecimal string representation for MD5 hash output formatting.

## Definition

```c
static void
bytesToHex(uint8 b[16], char *s)
```
## Detailed Description
This static utility function converts a 16-byte binary array (typically an MD5 hash digest) into a 32-character hexadecimal string representation followed by a null terminator. The function uses lowercase hexadecimal characters ('0'-'9', 'a'-'f') and processes each byte by extracting its high and low nibbles separately to produce two hexadecimal characters per byte.

The conversion process iterates through each of the 16 input bytes, splits each byte into its upper 4 bits and lower 4 bits, and maps these nibbles to their corresponding hexadecimal characters using a static lookup table.

## Parameters / Member Variables
- : Input array of 16 unsigned 8-bit bytes representing the binary data to convert (typically an MD5 digest)
- : Output character array that will receive the 32-character hexadecimal string plus null terminator (must be at least 33 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic C operations and static hex lookup table)
- Called from (representative examples):
  - [pg_md5_hash](../p/pg_md5_hash.md)

## Notes and Other Information
- The function is declared static, limiting its scope to the md5_common.c file
- Uses a static const hex lookup table "0123456789abcdef" for efficient character mapping
- Produces lowercase hexadecimal output
- The output string is always null-terminated
- Assumes the output buffer  has sufficient space (33 bytes minimum) - no bounds checking is performed
- This is a utility function specifically designed for MD5 hash formatting in PostgreSQL's cryptographic operations

## Simplified Source

```c
static void bytesToHex(uint8 b[16], char *s) {
    static const char *hex = "0123456789abcdef";
    int w = 0;

    // Convert each byte to two hex characters
    for (int q = 0; q < 16; q++) {
        s[w++] = hex[(b[q] >> 4) & 0x0F];  // High nibble
        s[w++] = hex[b[q] & 0x0F];         // Low nibble
    }

    s[w] = '\0';  // Null-terminate the string
}
```