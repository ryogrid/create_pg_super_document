# sha1_pad

## Location
src/common/sha1.c: 233 - 275

## Overview
Applies the required padding to the SHA-1 message according to the algorithm specification, ensuring the final message length is congruent to 448 modulo 512 bits before appending the 64-bit message length.

## Definition

```c
static void
sha1_pad(pg_sha1_ctx *ctx)
```
## Detailed Description
The  function implements the SHA-1 padding scheme as specified in FIPS PUB 180-1. It performs the final steps of message preparation before computing the hash digest:

1. **Initial padding**: Appends a single '1' bit (0x80 byte) to the message
2. **Zero padding**: Adds zero bytes to make room for the 64-bit length field
3. **Length appending**: Adds the original message length in bits as a 64-bit big-endian integer

The padding ensures the total message length becomes a multiple of 512 bits (64 bytes). If there isn't enough space in the current block for both padding and the 8-byte length field, it fills the current block with zeros, processes it, and continues padding in a new block.

The function handles endianness by appending the message length bytes in the correct order depending on the target architecture.

## Parameters / Member Variables
- : Pointer to the SHA-1 context structure containing:
  - : Message block buffer for padding operations
  - : 64-bit message length counter accessed as bytes
  - Message count and position tracking fields

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to append padding bytes and trigger block processing
  - : Macro for current position in message block
  - : Called when a block becomes full during padding
  - : To zero-fill padding regions

- Called from:
  - : During hash finalization to complete message processing

## Notes and Other Information
- This is a static function, only accessible within the sha1.c compilation unit  
- Handles both cases where padding fits in current block or requires an additional block
- Properly handles endianness for the 64-bit length field (big-endian in the hash)
- The padding scheme is critical for security - ensures no two different messages produce the same padded input
- After padding, the message length is always a multiple of 512 bits
- The 8-byte length field represents the original message length in bits, not the padded length