# pg_cryptohash_final

## Location
src/common/cryptohash_openssl.c: 255 - 325

## Overview
Finalizes a hash context and produces the final hash digest, with buffer length validation to ensure the destination buffer is large enough for the specific hash algorithm output.

## Definition
```c
int pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest, size_t len)
```

## Detailed Description
The pg_cryptohash_final function completes the hash calculation and writes the final digest to the destination buffer. It performs critical buffer length validation for each supported hash algorithm, ensuring the destination buffer is large enough to hold the complete hash digest. If the buffer is too small, it sets an appropriate error code in the context and returns failure.

The function acts as a dispatcher, calling the appropriate algorithm-specific finalization function based on the hash type stored in the context. Each algorithm has a specific digest length requirement that must be satisfied before the final hash can be computed and stored.

## Parameters / Member Variables
- `ctx`: Pointer to the cryptographic hash context containing the hash state and algorithm type
- `dest`: Pointer to the destination buffer where the final hash digest will be written
- `len`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_md5_final
  - pg_sha1_final
  - pg_sha224_final
  - pg_sha256_final
  - pg_sha384_final
  - pg_sha512_final
  - MD5_DIGEST_LENGTH, SHA1_DIGEST_LENGTH, PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH, PG_SHA512_DIGEST_LENGTH (digest length constants)
  - PG_CRYPTOHASH_ERROR_DEST_LEN (error code constant)
  - PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512 (enum constants)
  - pg_cryptohash_ctx (context structure)
- Called from (representative examples):
  - SendBackupManifest
  - scram_mock_salt
  - cryptohash_internal
  - pg_checksum_final
  - pg_hmac_final
  - pg_md5_hash

## Notes and Other Information
- Returns 0 on success, -1 on failure (null context or insufficient buffer length)
- Buffer length validation prevents buffer overflow attacks and ensures data integrity
- Sets ctx->error to PG_CRYPTOHASH_ERROR_DEST_LEN if destination buffer is too small
- Required minimum buffer sizes: MD5 (16 bytes), SHA-1 (20 bytes), SHA-224 (28 bytes), SHA-256 (32 bytes), SHA-384 (48 bytes), SHA-512 (64 bytes)
- The context becomes invalid after finalization and should not be reused without reinitialization
- Part of PostgreSQL's streaming hash interface used for incremental hash calculations