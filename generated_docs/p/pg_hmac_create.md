# pg_hmac_create

## Location
[src/common/hmac_openssl.c:122-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L122-L182)

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
  - [pg_cryptohash_create](pg_cryptohash_create.md)
  - [explicit_bzero](../e/explicit_bzero.md)
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
  - [verify_client_proof](../v/verify_client_proof.md)
  - [build_server_final_message](../b/build_server_final_message.md)
  - [scram_SaltedPassword](../s/scram_SaltedPassword.md)
  - [scram_ClientKey](../s/scram_ClientKey.md)
  - [scram_ServerKey](../s/scram_ServerKey.md)
  - [calculate_client_proof](../c/calculate_client_proof.md)
  - [verify_server_signature](../v/verify_server_signature.md)

## Notes and Other Information
- Defined in src/common/hmac.c, available to both frontend and backend code
- Returns NULL on allocation failure in frontend code
- [Backend](../B/Backend.md) code may issue an error and not return on OOM conditions
- Properly handles cleanup on partial initialization failures
- Uses explicit_bzero for secure memory clearing on error paths
- Part of PostgreSQL's cryptographic infrastructure, heavily used in SCRAM authentication

## Simplified Source

```c
pg_hmac_ctx *pg_hmac_create(pg_cryptohash_type type) {
    // Allocate and initialize context
    pg_hmac_ctx *ctx = ALLOC(sizeof(pg_hmac_ctx));
    if (ctx == NULL)
        return NULL;

    memset(ctx, 0, sizeof(pg_hmac_ctx));
    ctx->type = type;
    ctx->error = PG_HMAC_ERROR_NONE;
    ctx->errreason = NULL;

    // Set algorithm-specific parameters
    switch (type) {
        case PG_MD5:
            ctx->digest_size = MD5_DIGEST_LENGTH;
            ctx->block_size = MD5_BLOCK_SIZE;
            break;
        case PG_SHA1:
            ctx->digest_size = SHA1_DIGEST_LENGTH;
            ctx->block_size = SHA1_BLOCK_SIZE;
            break;
        case PG_SHA224:
            ctx->digest_size = PG_SHA224_DIGEST_LENGTH;
            ctx->block_size = PG_SHA224_BLOCK_LENGTH;
            break;
        case PG_SHA256:
            ctx->digest_size = PG_SHA256_DIGEST_LENGTH;
            ctx->block_size = PG_SHA256_BLOCK_LENGTH;
            break;
        case PG_SHA384:
            ctx->digest_size = PG_SHA384_DIGEST_LENGTH;
            ctx->block_size = PG_SHA384_BLOCK_LENGTH;
            break;
        case PG_SHA512:
            ctx->digest_size = PG_SHA512_DIGEST_LENGTH;
            ctx->block_size = PG_SHA512_BLOCK_LENGTH;
            break;
    }

    // Create underlying hash context
    ctx->hash = pg_cryptohash_create(type);
    if (ctx->hash == NULL) {
        explicit_bzero(ctx, sizeof(pg_hmac_ctx));
        FREE(ctx);
        return NULL;
    }

    return ctx;
}
```