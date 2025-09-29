# pg_sha256_final

## Location
[src/common/sha2.c:577-604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L577-L604)

## Overview
Finalizes SHA-256 hash computation by processing remaining buffered data and extracting the final digest value.

## Definition

```c
void
pg_sha256_final(pg_sha256_ctx *context, uint8 *digest)
```
## Detailed Description
The  function completes the SHA-256 hashing process by:
1. Calling  to process any remaining buffered data and finalize the hash state
2. Converting the hash state from network byte order to host byte order on little-endian systems
3. Copying the final digest to the output buffer if provided
4. Securely cleaning up the context structure by zeroing its contents

The function handles byte order conversion automatically based on the system's endianness, ensuring the digest is properly formatted regardless of the target architecture.

## Parameters / Member Variables
- : Pointer to the SHA-256 context structure containing the hash state
- : Output buffer to receive the final SHA-256 digest (32 bytes), or NULL to skip digest extraction

## Dependencies
- Functions called/Symbols referenced:
  - [SHA256_Last](../S/SHA256_Last.md)
  - REVERSE32
  - memcpy
  - memset
- Constants used:
  - PG_SHA256_DIGEST_LENGTH
- Called from (representative examples):
  - [pg_cryptohash_final](pg_cryptohash_final.md)

## Notes and Other Information
- The function safely handles NULL digest pointers by skipping the finalization process
- Context memory is always zeroed for security, regardless of whether a digest is requested
- Byte order conversion is conditionally compiled based on WORDS_BIGENDIAN macro
- Part of PostgreSQL's cryptographic hashing infrastructure for SHA-256 operations

## Simplified Source

```c
void pg_sha256_final(pg_sha256_ctx *context, uint8 *digest) {
    // Only proceed if digest buffer is provided
    if (digest != NULL) {
        // Finalize the SHA-256 computation with padding
        SHA256_Last(context);

        // Convert to host byte order on little-endian systems
        #ifndef WORDS_BIGENDIAN
        for (int j = 0; j < 8; j++) {
            REVERSE32(context->state[j], context->state[j]);
        }
        #endif

        // Copy the full 32-byte (256-bit) digest to output buffer
        memcpy(digest, context->state, PG_SHA256_DIGEST_LENGTH);
    }

    // Securely clear context data
    memset(context, 0, sizeof(pg_sha256_ctx));
}
```