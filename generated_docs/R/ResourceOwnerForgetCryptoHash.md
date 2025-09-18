# ResourceOwnerForgetCryptoHash

## Location
src/common/cryptohash_openssl.c: 96 - 102

## Overview
A convenience wrapper function that unregisters a cryptographic hash context from PostgreSQL's resource owner system, indicating that the resource is being explicitly managed and cleaned up.

## Definition
```c
static inline void ResourceOwnerForgetCryptoHash(ResourceOwner owner, pg_cryptohash_ctx *ctx)
```

## Detailed Description
This function serves as a wrapper around the generic `ResourceOwnerForget` function, specifically tailored for cryptographic hash contexts. It removes a hash context from the resource owner's tracking list, typically called when the context is being explicitly freed. This prevents the resource owner from attempting to clean up a resource that has already been properly disposed of, avoiding double-free errors and ensuring clean resource management.

The function is implemented as a static inline function in the cryptohash_openssl.c file, providing efficient type-safe resource management for cryptographic hash operations.

## Parameters / Member Variables
- `owner`: The ResourceOwner that is currently tracking this cryptographic hash context
- `ctx`: Pointer to the pg_cryptohash_ctx structure to be removed from tracking

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - PointerGetDatum
  - cryptohash_resowner_desc (resource descriptor)
- Called from (representative examples):
  - pg_cryptohash_free

## Notes and Other Information
- This is a static inline function defined in src/common/cryptohash_openssl.c:96-102
- Part of PostgreSQL's resource management system that ensures proper cleanup coordination
- Must be called before manually freeing cryptographic hash contexts to avoid resource management conflicts
- Works in conjunction with ResourceOwnerRememberCryptoHash to provide complete resource lifecycle management
- Uses the same specialized descriptor (cryptohash_resowner_desc) as its counterpart remember function