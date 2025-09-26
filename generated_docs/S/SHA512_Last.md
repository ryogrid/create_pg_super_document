# SHA512_Last

## Location
src/common/sha2.c: 855 - 904

## Overview
Completes SHA-512 hash computation by applying message padding and processing the final block(s) according to the SHA-512 specification.

## Definition


## Detailed Description
The  function implements the SHA-512 message padding and finalization process as specified in FIPS 180-4:

1. **Bit Count Conversion**: Converts the 128-bit message length counter from host byte order to big-endian format for consistent cross-platform results
2. **Message Padding**: Applies SHA-512 padding rules:
   - Appends a single '1' bit (0x80 byte) immediately after the message
   - Pads with zeros to achieve proper block alignment
   - Reserves space for the 128-bit message length at the end
3. **Block Processing Logic**:
   - If current data fits in one block (≤ 112 bytes): pads and processes single final block
   - If current data requires two blocks (> 112 bytes): processes current block, then a second padded block
   - For empty buffer: creates a new block starting with the padding bit
4. **Length Encoding**: Encodes the total message length in bits as a 128-bit big-endian integer in the final 16 bytes
5. **Final Transform**: Processes the final padded block through SHA512_Transform to complete the hash computation

The function ensures that the message length modulo 1024 bits equals 896 bits, leaving exactly 128 bits for the length field.

## Parameters / Member Variables
- : Pointer to SHA-512 context containing the current state, buffer, and bit count

## Dependencies
- Functions called/Symbols referenced:
  - SHA512_Transform
  - REVERSE64
  - memset
- Constants used:
  - PG_SHA512_BLOCK_LENGTH (128 bytes)
  - PG_SHA512_SHORT_BLOCK_LENGTH (112 bytes)
- Called from (representative examples):
  - pg_sha512_final
  - pg_sha384_final

## Notes and Other Information
- This is a static (internal) function not exposed outside the sha2.c module
- Implements the standard SHA-512 padding scheme: message + '1' bit + zeros + 128-bit length
- Handles both single-block and two-block padding scenarios automatically
- Length encoding is always in big-endian format regardless of host architecture
- After this function completes, the context->state array contains the final SHA-512 hash value
- Must be called exactly once per hash computation, typically by pg_sha512_final()
- The 128-bit length counter allows for messages up to 2^128-1 bits (2^125-1 bytes) in length