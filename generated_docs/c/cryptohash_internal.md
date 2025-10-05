# cryptohash_internal

## Location
[src/backend/utils/adt/cryptohashfuncs.c:80-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cryptohashfuncs.c#L80-L139)

## Overview
Internal utility function that computes various cryptographic hashes (SHA224, SHA256, SHA384, SHA512) for bytea input data and returns the result as a bytea value.

## Definition
```c
static inline bytea * cryptohash_internal(pg_cryptohash_type type, bytea *input)
```

## Detailed Description
This is a core internal function that provides a unified interface for computing different SHA cryptographic hash algorithms. It serves as the common implementation backend for the various SHA hash SQL functions (sha224_bytea, sha256_bytea, etc.). The function creates a cryptographic hash context, processes the input data through the hashing algorithm, and returns the raw binary hash result as a PostgreSQL bytea value. It supports SHA224, SHA256, SHA384, and SHA512 algorithms, while explicitly rejecting MD5 and SHA1 for security reasons.

## Parameters / Member Variables
- `type`: pg_cryptohash_type enum specifying which hash algorithm to use (PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512)
- `input`: Input bytea containing the binary data to be hashed
- `data`: Pointer to the raw input data extracted from the bytea
- `typestr`: String representation of the hash type for error messages
- `digest_len`: Length in bytes of the resulting hash digest
- `len`: Length of the input data in bytes
- `ctx`: Cryptographic hash context used by the pg_cryptohash API
- `result`: Output bytea containing the computed hash digest

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_create](../p/pg_cryptohash_create.md)
  - [pg_cryptohash_init](../p/pg_cryptohash_init.md)
  - [pg_cryptohash_update](../p/pg_cryptohash_update.md)
  - [pg_cryptohash_final](../p/pg_cryptohash_final.md)
  - [pg_cryptohash_free](../p/pg_cryptohash_free.md)
  - [pg_cryptohash_error](../p/pg_cryptohash_error.md)
  - [palloc0](../p/palloc0.md)
  - SET_VARSIZE
  - VARDATA
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
- Constants referenced:
  - PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512
  - PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH, PG_SHA512_DIGEST_LENGTH
  - VARHDRSZ
- Called from (representative examples):
  - [sha224_bytea](../s/sha224_bytea.md)
  - [sha256_bytea](../s/sha256_bytea.md)
  - [sha384_bytea](../s/sha384_bytea.md)
  - [sha512_bytea](../s/sha512_bytea.md)

## Notes and Other Information
- Located in src/backend/utils/adt/cryptohashfuncs.c:80-139
- Static inline function for internal use only within the cryptohash functions module
- Explicitly rejects MD5 and SHA1 hash types with error messages for security reasons
- Uses PostgreSQLs pg_cryptohash API for secure hash computation
- Returns raw binary hash data as bytea, not hexadecimal strings
- Handles memory management with palloc0 and proper VARHDRSZ sizing
- Comprehensive error handling for all stages of hash computation (init, update, final)
- Serves as the common backend for all supported SHA hash SQL functions

## Simplified Source

```c
static inline bytea * cryptohash_internal(pg_cryptohash_type type, bytea *input) {
    const uint8 *data;
    const char *typestr = NULL;
    int digest_len = 0;
    size_t len;
    pg_cryptohash_ctx *ctx;
    bytea *result;

    // Determine hash type parameters
    switch (type) {
        case PG_SHA224:
            typestr = "SHA224";
            digest_len = PG_SHA224_DIGEST_LENGTH;
            break;
        case PG_SHA256:
            typestr = "SHA256";
            digest_len = PG_SHA256_DIGEST_LENGTH;
            break;
        case PG_SHA384:
            typestr = "SHA384";
            digest_len = PG_SHA384_DIGEST_LENGTH;
            break;
        case PG_SHA512:
            typestr = "SHA512";
            digest_len = PG_SHA512_DIGEST_LENGTH;
            break;
        case PG_MD5:
        case PG_SHA1:
            elog(ERROR, "unsupported cryptohash type %d", type);
            break;
    }

    // Allocate result buffer and extract input data
    result = palloc0(digest_len + VARHDRSZ);
    len = VARSIZE_ANY_EXHDR(input);
    data = (unsigned char *) VARDATA_ANY(input);

    // Create hash context and compute hash
    ctx = pg_cryptohash_create(type);
    if (pg_cryptohash_init(ctx) < 0)
        elog(ERROR, "could not initialize %s context: %s", typestr, pg_cryptohash_error(ctx));
    if (pg_cryptohash_update(ctx, data, len) < 0)
        elog(ERROR, "could not update %s context: %s", typestr, pg_cryptohash_error(ctx));
    if (pg_cryptohash_final(ctx, (unsigned char *) VARDATA(result), digest_len) < 0)
        elog(ERROR, "could not finalize %s context: %s", typestr, pg_cryptohash_error(ctx));

    pg_cryptohash_free(ctx);
    SET_VARSIZE(result, digest_len + VARHDRSZ);

    return result;
}
```