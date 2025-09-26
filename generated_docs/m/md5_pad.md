# md5_pad

## Location
[src/common/md5.c:310-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L310-L347)

## Overview
Handles the padding phase of MD5 computation by appending the required padding bits and length information to complete the final block(s) for processing.

## Definition
```c
static void md5_pad(pg_md5_ctx *ctx)
```

## Detailed Description
The `md5_pad` function implements the MD5 padding algorithm as specified in RFC 1321. MD5 requires that input messages be padded to a length that is congruent to 448 bits modulo 512 bits (56 bytes modulo 64 bytes), followed by a 64-bit representation of the original message length. This ensures that the total padded message length is always a multiple of 512 bits (64 bytes).

The function calculates the gap remaining in the current 64-byte buffer and fills it with padding data from `md5_paddat`. If there's insufficient space (8 bytes or less) to fit both padding and the 8-byte length field, it processes the current buffer, starts a new buffer with remaining padding, and then appends the length. The 64-bit message length is stored in little-endian format, with big-endian systems requiring explicit byte reordering.

## Parameters / Member Variables
- `ctx`: Pointer to the MD5 context structure containing the current buffer state and message length information

## Dependencies
- Functions called/Symbols referenced:
  - MD5_BUFLEN (constant defining MD5 buffer length - 64 bytes)
  - [md5_calc](md5_calc.md) (core MD5 computation function)
  - md5_paddat (external padding data array)
  - memmove (standard library function for memory copying)
- Called from (representative examples):
  - [pg_md5_final](../p/pg_md5_final.md)

## Notes and Other Information
- This function is static and only used internally within the MD5 implementation
- The padding always begins with a single '1' bit (0x80 byte) followed by zero or more '0' bits
- The final 8 bytes always contain the original message length in bits, stored as a 64-bit little-endian integer
- Big-endian systems require manual byte swapping for the length field to maintain MD5 specification compliance
- May call md5_calc twice if padding spans across buffer boundaries
- Critical for ensuring MD5 algorithm correctness - improper padding would produce incorrect hash values