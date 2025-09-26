# sha1_result

## Location
src/common/sha1.c: 276 - 315

## Overview
Extracts the final 20-byte SHA-1 hash digest from the context's hash state, handling endianness conversion to produce the standard big-endian output format.

## Definition


## Detailed Description
The  function copies the computed SHA-1 hash value from the internal context structure to the output buffer. Since SHA-1 produces a 160-bit (20-byte) hash, the function transfers exactly 20 bytes from the context's hash state array to the destination buffer.

The function handles endianness conversion to ensure the output digest conforms to the SHA-1 standard big-endian format:
- **Big-endian systems**: Direct memory copy from hash state to output
- **Little-endian systems**: Byte-wise reversal of each 32-bit hash word to convert from internal little-endian representation to standard big-endian output

This ensures the SHA-1 digest is identical across different architectures and matches standard test vectors.

## Parameters / Member Variables
- : Pointer to the output buffer where the 20-byte SHA-1 digest will be stored
- : Pointer to the SHA-1 context structure containing:
  - : Hash state array accessed as individual bytes for endianness handling

## Dependencies
- Functions called/Symbols referenced:
  - : Used on big-endian systems for direct hash state copy
  - : Context structure type containing hash state

- Called from:
  - : After message padding is complete, to extract the final hash

## Notes and Other Information
- This is a static function, only accessible within the sha1.c compilation unit
- Produces exactly 20 bytes of output (160-bit SHA-1 hash)
- Output format is always big-endian regardless of host architecture
- The function assumes the hash computation is complete and the context contains valid hash state
- Does not modify the context structure, only reads the hash state
- The output buffer must be pre-allocated with at least 20 bytes of space
- Critical for ensuring SHA-1 compatibility across different hardware platforms