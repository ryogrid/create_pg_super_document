# pg_cryptohash_free

## Location
[src/common/cryptohash_openssl.c:326-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L326-L348)

## Overview
Securely frees a cryptographic hash context, clearing all sensitive data from memory before deallocating the context structure.

## Definition
```c
void pg_cryptohash_free(pg_cryptohash_ctx *ctx)
```

## Detailed Description
The pg_cryptohash_free function safely deallocates a cryptographic hash context by first securely clearing all data in the context structure and then freeing the allocated memory. The function uses explicit_bzero to ensure that sensitive cryptographic state information is completely removed from memory, preventing potential security vulnerabilities where hash state data might persist in freed memory and be accessible to attackers.

This function is essential for proper cleanup of cryptographic operations and follows security best practices by ensuring that no cryptographic material remains in memory after the context is no longer needed.

## Parameters / Member Variables
- `ctx`: Pointer to the cryptographic hash context to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [explicit_bzero](../e/explicit_bzero.md) (secure memory clearing function)
  - FREE (memory deallocation macro)
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md) (context structure type)
- Called from (representative examples):
  - [FreeBackupManifest](../F/FreeBackupManifest.md)
  - [scram_mock_salt](../s/scram_mock_salt.md)
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - [pg_checksum_init](pg_checksum_init.md) (error cleanup)
  - [pg_checksum_final](pg_checksum_final.md)
  - [ResOwnerReleaseCryptoHash](../R/ResOwnerReleaseCryptoHash.md)
  - [pg_hmac_free](pg_hmac_free.md)
  - [pg_md5_hash](pg_md5_hash.md)

## Notes and Other Information
- Safe to call with NULL pointer (function returns immediately)
- Uses explicit_bzero for secure memory clearing to prevent sensitive data from persisting in freed memory
- Should be called for every successfully created hash context to prevent memory leaks
- Part of PostgreSQL's secure memory management practices for cryptographic operations
- The context becomes completely invalid after calling this function
- Used extensively in error cleanup paths to ensure proper resource cleanup even when operations fail
- Critical for security: prevents potential information disclosure through memory analysis attacks

## Simplified Source

```c
// Securely free a cryptographic hash context
void pg_cryptohash_free(pg_cryptohash_ctx *ctx)
{
    // Allow NULL pointer (safe to call)
    if (ctx == NULL)
        return;

    // Securely clear all sensitive data in the context
    explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));

    // Free the memory
    FREE(ctx);
}
```