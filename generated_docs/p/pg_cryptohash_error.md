# pg_cryptohash_error

## Location
[src/common/cryptohash_openssl.c:349-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L349-L382)

## Overview
Retrieves a human-readable error message describing the last error that occurred during cryptographic hash operations on a given context.

## Definition
```c
const char *pg_cryptohash_error(pg_cryptohash_ctx *ctx)
```

## Detailed Description
The pg_cryptohash_error function provides diagnostic information about errors that occur during cryptographic hash operations. It examines the error state stored in the hash context and returns an appropriate localized error message. The function handles two main error conditions: out-of-memory errors (when ctx is NULL) and buffer size errors (when the destination buffer is too small for the hash output).

The function returns statically allocated, localized strings that describe the specific error condition. The error messages are marked for internationalization using the _() macro, making them translatable for different locales.

## Parameters / Member Variables
- `ctx`: Pointer to the cryptographic hash context (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_CRYPTOHASH_ERROR_NONE (no error constant)
  - PG_CRYPTOHASH_ERROR_DEST_LEN (destination buffer too small error constant)
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md) (context structure)
  - _() (internationalization macro for string translation)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - [InitializeBackupManifest](../I/InitializeBackupManifest.md)
  - [SendBackupManifest](../S/SendBackupManifest.md)
  - [AppendStringToManifest](../A/AppendStringToManifest.md)
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - [pg_hmac_init](pg_hmac_init.md)
  - [pg_hmac_update](pg_hmac_update.md)
  - [pg_hmac_final](pg_hmac_final.md)
  - [pg_md5_hash](pg_md5_hash.md)
  - [pg_md5_binary](pg_md5_binary.md)
  - [scram_H](../s/scram_H.md)

## Notes and Other Information
- Returns localized error messages suitable for display to users
- Safe to call with NULL context (returns "out of memory" message)
- Error messages are statically allocated and do not need to be freed
- The function covers the primary error conditions in the cryptographic hash system
- Used extensively throughout PostgreSQL for error reporting in cryptographic operations
- Contains an assertion to catch unexpected error states during development
- Part of PostgreSQL's comprehensive error handling and reporting system for cryptographic operations
- Error state is maintained in the context until the context is freed or reused

## Simplified Source

```c
const char *
pg_cryptohash_error(pg_cryptohash_ctx *ctx)
{
    if (ctx == NULL)
        return _("out of memory");

    switch (ctx->error)
    {
        case PG_CRYPTOHASH_ERROR_NONE:
            return _("success");
        case PG_CRYPTOHASH_ERROR_DEST_LEN:
            return _("destination buffer too small");
    }

    Assert(false);
    return _("success");
}
```