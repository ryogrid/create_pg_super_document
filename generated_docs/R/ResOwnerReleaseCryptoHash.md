# ResOwnerReleaseCryptoHash

## Location
src/common/cryptohash_openssl.c: 383 - 390

## Overview
A resource owner callback function that handles automatic cleanup of cryptographic hash contexts when PostgreSQL's resource management system releases resources.

## Definition
```c
static void ResOwnerReleaseCryptoHash(Datum res)
```

## Detailed Description
The ResOwnerReleaseCryptoHash function is a callback function used by PostgreSQL's resource owner system to automatically clean up cryptographic hash contexts when resources need to be released. This typically occurs during transaction cleanup, error recovery, or session termination. The function ensures that cryptographic hash contexts are properly freed even if the application code doesn't explicitly call cleanup functions.

The function first clears the resource owner reference in the context to prevent circular references, then calls pg_cryptohash_free to perform the actual cleanup including secure memory clearing and deallocation.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the pg_cryptohash_ctx to be released

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_free](../p/pg_cryptohash_free.md) (cleanup function for hash contexts)
  - [pg_cryptohash_ctx](../p/pg_cryptohash_ctx.md) (context structure type)
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro to extract pointer from Datum)
- Called from (representative examples):
  - PostgreSQL resource owner system during resource cleanup
  - Referenced by pg_cryptohash_ctx structure initialization

## Notes and Other Information
- Static function with internal linkage, only accessible within cryptohash_openssl.c
- Part of PostgreSQL's resource management system for automatic cleanup
- Ensures cryptographic contexts are properly cleaned up even during error conditions
- Prevents memory leaks and security issues by guaranteeing context cleanup
- Sets resowner field to NULL before freeing to avoid dangling references
- Critical for robust resource management in long-running PostgreSQL processes
- Only used in the OpenSSL implementation of cryptographic hash functions
- Integrates with PostgreSQL's broader resource tracking and cleanup infrastructure