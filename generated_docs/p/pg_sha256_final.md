# pg_sha256_final

## Location
src/common/sha2.c: 577 - 604

## Overview
Finalizes SHA-256 hash computation by processing remaining buffered data and extracting the final digest value.

## Definition


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
  - SHA256_Last
  - REVERSE32
  - memcpy
  - memset
- Constants used:
  - PG_SHA256_DIGEST_LENGTH
- Called from (representative examples):
  - pg_cryptohash_final

## Notes and Other Information
- The function safely handles NULL digest pointers by skipping the finalization process
- Context memory is always zeroed for security, regardless of whether a digest is requested
- Byte order conversion is conditionally compiled based on WORDS_BIGENDIAN macro
- Part of PostgreSQL's cryptographic hashing infrastructure for SHA-256 operations