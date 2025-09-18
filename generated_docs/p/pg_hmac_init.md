# pg_hmac_init

## Location
src/common/hmac_openssl.c: 183 - 228

## Overview
Initializes an HMAC context with a secret key, preparing it for message authentication operations using the HMAC algorithm.

## Definition
```c
int pg_hmac_init(pg_hmac_ctx *ctx, const uint8 *key, size_t len)
```

## Detailed Description
pg_hmac_init implements the HMAC (Hash-based Message Authentication Code) initialization algorithm as defined in RFC 2104. It takes a secret key and prepares the HMAC context for subsequent update and finalization operations. The function handles key preprocessing: if the key is longer than the hash algorithm's block size, it first hashes the key to reduce it to the digest size. It then creates the inner and outer padding values (ipad and opad) by XORing the key with standard HMAC constants (0x36 and 0x5C). Finally, it initializes the underlying hash context with the inner padding, setting up for the first phase of HMAC computation.

## Parameters / Member Variables
- `ctx`: Pointer to the HMAC context structure to initialize
- `key`: Pointer to the secret key bytes
- `len`: Length of the secret key in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ALLOC (memory allocation macro)
  - FREE (memory deallocation macro)
  - pg_cryptohash_create
  - pg_cryptohash_init
  - pg_cryptohash_update
  - pg_cryptohash_final
  - pg_cryptohash_free
  - pg_cryptohash_error
  - memset
  - explicit_bzero
- Constants referenced:
  - HMAC_IPAD (0x36)
  - HMAC_OPAD (0x5C)
  - PG_HMAC_ERROR_OOM
  - PG_HMAC_ERROR_INTERNAL
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
- Returns 0 on success, -1 on failure
- Implements RFC 2104 HMAC key preprocessing and padding generation
- Handles keys longer than block size by hashing them first
- Sets appropriate error codes in ctx->error on failure
- Uses temporary buffer for key shrinking that is securely cleared
- Critical component of PostgreSQL's SCRAM-SHA authentication implementation
- Must be called after pg_hmac_create and before pg_hmac_update operations