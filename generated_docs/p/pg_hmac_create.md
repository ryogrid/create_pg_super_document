# pg_hmac_create

## Location
src/common/hmac_openssl.c: 122 - 182

## Overview
Allocates and initializes a new HMAC context structure for the specified cryptographic hash algorithm type.

## Definition
```c
pg_hmac_ctx *pg_hmac_create(pg_cryptohash_type type)
```

## Detailed Description
pg_hmac_create is the primary constructor function for HMAC contexts in PostgreSQL. It allocates memory for a new pg_hmac_ctx structure, initializes it with algorithm-specific parameters (digest size and block size), and creates an underlying cryptographic hash context. The function supports multiple hash algorithms including MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512. If allocation fails or the underlying hash context creation fails, the function performs proper cleanup and returns NULL. In backend environments, out-of-memory conditions may trigger an error rather than returning NULL.

## Parameters / Member Variables
- `type`: The cryptographic hash algorithm type (pg_cryptohash_type enum: PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512)

## Dependencies
- Functions called/Symbols referenced:
  - ALLOC (memory allocation macro)
  - FREE (memory deallocation macro)
  - pg_cryptohash_create
  - explicit_bzero
  - memset
- Constants referenced:
  - MD5_DIGEST_LENGTH, MD5_BLOCK_SIZE
  - SHA1_DIGEST_LENGTH, SHA1_BLOCK_SIZE
  - PG_SHA224_DIGEST_LENGTH, PG_SHA224_BLOCK_LENGTH
  - PG_SHA256_DIGEST_LENGTH, PG_SHA256_BLOCK_LENGTH
  - PG_SHA384_DIGEST_LENGTH, PG_SHA384_BLOCK_LENGTH
  - PG_SHA512_DIGEST_LENGTH, PG_SHA512_BLOCK_LENGTH
  - PG_HMAC_ERROR_NONE
- Called from (representative examples):
  - verify_client_proof
  - build_server_final_message
  - scram_SaltedPassword
  - scram_ClientKey
  - scram_ServerKey
  - calculate_client_proof
  - verify_server_signature

## Notes and Other Information
- Defined in src/common/hmac.c, available to both frontend and backend code
- Returns NULL on allocation failure in frontend code
- Backend code may issue an error and not return on OOM conditions
- Properly handles cleanup on partial initialization failures
- Uses explicit_bzero for secure memory clearing on error paths
- Part of PostgreSQL's cryptographic infrastructure, heavily used in SCRAM authentication