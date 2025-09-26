# pg_sha384_final

## Location
src/common/sha2.c: 950 - 977

## Overview
Finalizes SHA-384 hash computation by processing remaining data, outputting the final 48-byte hash digest, and securely clearing the context structure.

## Definition
```c
void pg_sha384_final(pg_sha384_ctx *context, uint8 *digest)
```

## Detailed Description
The `pg_sha384_final` function completes the SHA-384 hashing process by performing the following operations:

1. **Final Processing**: Calls `SHA512_Last()` after casting the context to SHA-512 type, leveraging the shared finalization logic
2. **Byte Order Conversion**: On little-endian systems, converts only the first 6 state words (384 bits) from host byte order to big-endian format using the `REVERSE64` macro
3. **Digest Output**: Copies 48 bytes (384 bits) from the context's state array to the provided digest buffer, which is exactly 6 × 64-bit words
4. **Secure Cleanup**: Zeros out the entire context structure to prevent potential information leakage

The key difference from `pg_sha512_final` is that SHA-384 only processes the first 6 words of the 8-word state array for output, producing a 384-bit digest instead of 512 bits. The function handles both successful completion scenarios and cleanup-only scenarios when digest is NULL.

## Parameters / Member Variables
- `context`: Pointer to the SHA-384 context structure containing the current hash state and any buffered input data
- `digest`: Output buffer to receive the final 48-byte SHA-384 hash digest, or NULL if only cleanup is desired

## Dependencies
- Functions called/Symbols referenced:
  - `SHA512_Last` - Processes final data block and applies padding (shared with SHA-512)
  - `REVERSE64` - Macro for 64-bit byte order reversal on little-endian systems
  - `memcpy` - Copies first 6 words of hash state to output digest buffer
  - `memset` - Securely clears the context structure
- Referenced types/constants:
  - `pg_sha384_ctx` - SHA-384 context structure type
  - `pg_sha512_ctx` - SHA-512 context type (used for casting to share finalization logic)
  - `PG_SHA384_DIGEST_LENGTH` - Constant defining 48-byte digest length
- Called from (representative examples):
  - `pg_cryptohash_final` - Generic cryptographic hash finalization wrapper

## Notes and Other Information
- Only processes the first 6 words (384 bits) of the 8-word state array, unlike SHA-512 which uses all 8 words
- The context casting to `pg_sha512_ctx*` is safe because both context types have identical memory layouts
- Always clears the context structure with `memset`, ensuring sensitive data cannot be recovered after finalization
- On little-endian architectures, byte order conversion is required because SHA-384 specification mandates big-endian output format
- The digest parameter can be NULL, allowing the function to be used solely for secure context cleanup
- Follows RFC 6234 specifications for SHA-384 hash computation
- Part of PostgreSQL's internal cryptographic library and should not be called directly by user code