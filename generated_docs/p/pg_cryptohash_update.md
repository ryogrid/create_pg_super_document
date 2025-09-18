# pg_cryptohash_update

## Location
[src/common/cryptohash_openssl.c:230-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L230-L254)

## Overview
Updates a hash context with new data, supporting multiple hash algorithms including MD5, SHA-1, and SHA-2 family algorithms (SHA-224, SHA-256, SHA-384, SHA-512).

## Definition


## Detailed Description
The pg_cryptohash_update function is a generic interface for updating cryptographic hash contexts with new input data. It acts as a dispatcher that calls the appropriate algorithm-specific update function based on the hash type stored in the context. The function is designed to be used as part of a streaming hash calculation where data is processed incrementally rather than all at once.

The function performs input validation by checking for null context and then uses a switch statement to delegate to the appropriate hash algorithm implementation. All supported hash algorithms follow the same interface pattern, making this function a clean abstraction layer.

## Parameters / Member Variables
- : Pointer to the cryptographic hash context containing the hash state and algorithm type
- : Pointer to the input data buffer to be processed
- : Number of bytes to read from the data buffer

## Dependencies
- Functions called/Symbols referenced:
  - pg_md5_update
  - pg_sha1_update  
  - pg_sha224_update
  - pg_sha256_update
  - pg_sha384_update
  - pg_sha512_update
  - PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512 (enum constants)
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md) (context structure)
- Called from (representative examples):
  - [AppendStringToManifest](../A/AppendStringToManifest.md)
  - [scram_mock_salt](../s/scram_mock_salt.md)
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - pg_checksum_update
  - [pg_hmac_update](pg_hmac_update.md)
  - [pg_md5_hash](pg_md5_hash.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure (currently only fails if ctx is NULL)
- The function supports incremental hashing, allowing large datasets to be processed in chunks
- All algorithm-specific update functions follow the same signature pattern
- Part of PostgreSQL's common cryptographic hash interface used throughout the system
- The context must be properly initialized with pg_cryptohash_init before calling this function