# encode_varbyte

## Location
src/backend/access/gin/ginpostinglist.c: 115 - 132

## Overview
Encodes a 64-bit unsigned integer using variable-length byte encoding, storing the result in a byte buffer and advancing the buffer pointer.

## Definition
```c
static void encode_varbyte(uint64 val, unsigned char **ptr)
```

## Detailed Description
This function implements variable-length byte encoding (varbyte encoding) for 64-bit unsigned integers. The encoding uses the most significant bit of each byte as a continuation bit: if set to 1, it indicates that more bytes follow; if set to 0, it indicates the last byte of the encoded value. The remaining 7 bits of each byte store the actual data. This encoding is space-efficient for smaller values, using fewer bytes for smaller numbers while still supporting the full 64-bit range when needed.

The function processes the value in 7-bit chunks, encoding them from least significant to most significant bits, and updates the buffer pointer to point to the next available position.

## Parameters / Member Variables
- `val`: A 64-bit unsigned integer value to be encoded
- `ptr`: A pointer to a pointer to unsigned char buffer where the encoded bytes will be stored; this pointer is advanced to point after the encoded data

## Dependencies
- Functions called/Symbols referenced:
  - (No function calls - uses only basic operations)
- Called from (representative examples):
  - ginCompressPostingList

## Notes and Other Information
- This is a static function used internally for GIN index posting list compression
- The encoding format is: each byte contains 7 bits of data and 1 continuation bit (MSB)
- Continuation bit = 1 means more bytes follow, continuation bit = 0 means this is the last byte
- The value is encoded in little-endian fashion (least significant 7-bit chunks first)
- This variable-length encoding saves space when storing many small integers in posting lists
- The maximum encoded length for a 64-bit value is 10 bytes (9 bytes with continuation bit + 1 final byte)