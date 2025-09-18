# ResourceOwnerForgetHMAC

## Location
[src/common/hmac_openssl.c:96-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L96-L102)

## Overview
A convenience wrapper function that unregisters an HMAC context from PostgreSQL's resource owner mechanism during cleanup operations.

## Definition
```c
static inline void ResourceOwnerForgetHMAC(ResourceOwner owner, pg_hmac_ctx *ctx)
```

## Detailed Description
ResourceOwnerForgetHMAC is a static inline wrapper function that removes an HMAC context from PostgreSQL's resource owner tracking system. It calls the underlying ResourceOwnerForget function to unregister the given HMAC context pointer from the resource owner using the hmac_resowner_desc descriptor. This function is typically called during explicit cleanup operations before manually freeing HMAC contexts to prevent double-free errors.

## Parameters / Member Variables
- `owner`: The ResourceOwner that currently tracks the HMAC context
- `ctx`: Pointer to the pg_hmac_ctx structure to be untracked

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - [pg_hmac_ctx](../p/pg_hmac_ctx.md) (type)
  - ResourceOwner (type)
- Called from (representative examples):
  - [pg_hmac_free](../p/pg_hmac_free.md)

## Notes and Other Information
- This is a static inline function defined in src/common/hmac_openssl.c
- Part of PostgreSQL's resource management system for HMAC contexts
- Paired with ResourceOwnerRememberHMAC for complete resource lifecycle management
- Must be called before manually freeing HMAC contexts to maintain resource tracking consistency
- Uses the hmac_resowner_desc descriptor to identify the resource type