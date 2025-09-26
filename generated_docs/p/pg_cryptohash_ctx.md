# pg_cryptohash_ctx

## Location
[src/common/cryptohash_openssl.c:63-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L63-L90)

## Overview
The `pg_cryptohash_ctx` structure is the internal context structure for PostgreSQL cryptographic hash operations, serving as a unified interface for multiple hash algorithms including MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512.

## Definition
```c
struct pg_cryptohash_ctx
{
    pg_cryptohash_type type;
    pg_cryptohash_errno error;

    union
    {
        pg_md5_ctx      md5;
        pg_sha1_ctx     sha1;
        pg_sha224_ctx   sha224;
        pg_sha256_ctx   sha256;
        pg_sha384_ctx   sha384;
        pg_sha512_ctx   sha512;
    }               data;
};
```

## Detailed Description
The `pg_cryptohash_ctx` structure provides a generic cryptographic hash context that can handle multiple hash algorithms through a union-based design. This structure is private to the cryptohash implementation and is not exposed directly to client code. Instead, it is accessed through opaque pointers and a set of API functions.

The structure uses a union to store algorithm-specific context data, allowing for memory-efficient storage while supporting all major hash algorithms. The type field identifies which algorithm is being used, while the error field tracks any error conditions that occur during hash operations.

This design allows PostgreSQL to provide a unified hash interface regardless of the underlying implementation (built-in fallback implementations or OpenSSL-based implementations), making it easier to switch between different cryptographic backends.

## Parameters / Member Variables
- `type`: Enum value of type `pg_cryptohash_type` that specifies which hash algorithm is being used (PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, or PG_SHA512)
- `error`: Enum value of type `pg_cryptohash_errno` that tracks error states (PG_CRYPTOHASH_ERROR_NONE for success, PG_CRYPTOHASH_ERROR_DEST_LEN for destination buffer length errors)
- `data`: Union containing algorithm-specific context structures for each supported hash type (md5, sha1, sha224, sha256, sha384, sha512)

## Dependencies
- Functions called/Symbols referenced:
  - `pg_cryptohash_type` (enum defining supported hash algorithms)
  - `pg_cryptohash_errno` (enum defining error states)
  - `[pg_md5_ctx](pg_md5_ctx.md)` (MD5 context structure)
  - `[pg_sha1_ctx](pg_sha1_ctx.md)` (SHA-1 context structure)
  - `[pg_sha224_ctx](pg_sha224_ctx.md)` (SHA-224 context structure)
  - `[pg_sha256_ctx](pg_sha256_ctx.md)` (SHA-256 context structure)
  - `[pg_sha384_ctx](pg_sha384_ctx.md)` (SHA-384 context structure)
  - `[pg_sha512_ctx](pg_sha512_ctx.md)` (SHA-512 context structure)

- Called from (representative examples):
  - [pg_cryptohash_create](pg_cryptohash_create.md) (allocates and initializes context)
  - [pg_cryptohash_init](pg_cryptohash_init.md) (initializes hash computation)
  - [pg_cryptohash_update](pg_cryptohash_update.md) (processes data chunks)
  - [pg_cryptohash_final](pg_cryptohash_final.md) (finalizes hash and retrieves result)
  - [pg_cryptohash_free](pg_cryptohash_free.md) (deallocates context)
  - [pg_cryptohash_error](pg_cryptohash_error.md) (retrieves error information)
  - [scram_mock_salt](../s/scram_mock_salt.md) (SCRAM authentication)
  - [cryptohash_internal](../c/cryptohash_internal.md) (internal cryptographic operations)
  - [pg_hmac_init](pg_hmac_init.md) (HMAC operations)
  - [pg_md5_hash](pg_md5_hash.md) (MD5 hashing convenience functions)

## Notes and Other Information
- This structure is internal to the cryptohash implementation and should not be accessed directly by client code
- The structure is designed to work with both built-in fallback implementations and OpenSSL-based implementations
- Memory allocation strategy differs between backend (uses palloc/pfree) and frontend (uses malloc/free) contexts
- The union design ensures efficient memory usage while supporting multiple hash algorithms
- Error handling is integrated into the structure to provide detailed error information to callers
- The structure is used extensively in PostgreSQL authentication (SCRAM), backup manifest verification, and general cryptographic operations