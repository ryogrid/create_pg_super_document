# pg_cryptohash_init

## Location
[src/common/cryptohash_openssl.c:178-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L178-L229)

## Overview
Initializes a cryptographic hash context for the specified hash algorithm, preparing it for data input through update operations.

## Definition
```c
int pg_cryptohash_init(pg_cryptohash_ctx *ctx)
```

## Detailed Description
This function initializes a previously created cryptographic hash context, setting up the internal state for the specific hash algorithm (MD5, SHA1, SHA224, SHA256, SHA384, or SHA512). The implementation varies significantly between the generic and OpenSSL versions:

**Generic Implementation (cryptohash.c)**:
- Uses a switch statement to call the appropriate algorithm-specific initialization function
- Directly initializes the hash context based on the ctx->type field
- Always returns 0 on success (no failure cases in the generic implementation)

**OpenSSL Implementation (cryptohash_openssl.c)**:
- Uses OpenSSL's EVP_DigestInit_ex() function with the appropriate EVP hash function
- Handles OpenSSL-specific error conditions and error reporting
- Sets detailed error information in case of failure
- Clears the OpenSSL error queue after handling failures

The function must be called after pg_cryptohash_create() and before any pg_cryptohash_update() calls. It prepares the context for accepting data to be hashed.

## Parameters / Member Variables
- `ctx`: Pointer to a previously allocated pg_cryptohash_ctx structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - **Generic version**: pg_md5_init, pg_sha1_init, pg_sha224_init, pg_sha256_init, pg_sha384_init, pg_sha512_init
  - **OpenSSL version**: EVP_DigestInit_ex, EVP_md5, EVP_sha1, EVP_sha224, EVP_sha256, EVP_sha384, EVP_sha512, SSLerrmessage, ERR_get_error, ERR_clear_error
- Called from (representative examples):
  - [InitializeBackupManifest](../I/InitializeBackupManifest.md)
  - [scram_mock_salt](../s/scram_mock_salt.md)
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - pg_checksum_init
  - [pg_hmac_init](pg_hmac_init.md)
  - [pg_md5_hash](pg_md5_hash.md)
  - scram_H

## Notes and Other Information
- Two implementations exist: generic (src/common/cryptohash.c:100-135) and OpenSSL-specific (src/common/cryptohash_openssl.c:177-222)
- Returns 0 on success and -1 on failure
- The OpenSSL version provides detailed error reporting through ctx->errreason and ctx->error fields
- In OpenSSL builds, FIPS-enabled configurations may generate additional errors that are automatically cleared
- Must be called on a context created with pg_cryptohash_create() before any update operations
- The generic version does not validate the context type beyond the switch statement
- OpenSSL version handles the complexity of mapping PostgreSQL hash types to OpenSSL EVP functions