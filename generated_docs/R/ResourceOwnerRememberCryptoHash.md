# ResourceOwnerRememberCryptoHash

## Location
[src/common/cryptohash_openssl.c:91-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/cryptohash_openssl.c#L91-L95)

## Overview
A convenience wrapper function that registers a cryptographic hash context with PostgreSQL's resource owner system for automatic cleanup upon transaction abort or process exit.

## Definition

```c
static inline void
ResourceOwnerRememberCryptoHash(ResourceOwner owner, pg_cryptohash_ctx *ctx)
```
## Detailed Description
This function serves as a wrapper around the generic  function, specifically tailored for cryptographic hash contexts. It registers a hash context with the resource owner system so that the context will be automatically cleaned up if the owning transaction aborts or the process terminates unexpectedly. This prevents resource leaks and ensures proper cleanup of OpenSSL cryptographic resources.

The function is implemented as a static inline function in the cryptohash_openssl.c file, making it efficient while providing type-safe resource management for cryptographic hash operations.

## Parameters / Member Variables
- : The ResourceOwner that will track this cryptographic hash context
- : Pointer to the pg_cryptohash_ctx structure to be tracked for automatic cleanup

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - cryptohash_resowner_desc (resource descriptor)
- Called from (representative examples):
  - [pg_cryptohash_create](../p/pg_cryptohash_create.md)

## Notes and Other Information
- This is a static inline function defined in src/common/cryptohash_openssl.c:91-95
- Part of PostgreSQL's resource management system that ensures cryptographic contexts are properly cleaned up
- Uses the generic resource owner infrastructure with a specialized descriptor for cryptohash contexts
- Typically called when creating new cryptographic hash contexts to ensure they're tracked for cleanup