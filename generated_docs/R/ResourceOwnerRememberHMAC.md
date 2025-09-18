# ResourceOwnerRememberHMAC

## Location
[src/common/hmac_openssl.c:91-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L91-L95)

## Overview
A convenience wrapper function that registers an HMAC context with PostgreSQL's resource owner mechanism for automatic memory management.

## Definition


## Detailed Description
ResourceOwnerRememberHMAC is a static inline wrapper function that integrates HMAC context memory management with PostgreSQL's resource owner system. It calls the underlying ResourceOwnerRemember function, registering the given HMAC context pointer with the resource owner using the hmac_resowner_desc descriptor. This ensures that the HMAC context will be automatically cleaned up when the resource owner is released, preventing memory leaks in error scenarios or during normal cleanup operations.

## Parameters / Member Variables
- `owner`: The ResourceOwner that will take responsibility for the HMAC context
- `ctx`: Pointer to the pg_hmac_ctx structure to be tracked

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - [pg_hmac_ctx](../p/pg_hmac_ctx.md) (type)
  - ResourceOwner (type)
- Called from (representative examples):
  - [pg_hmac_create](../p/pg_hmac_create.md)

## Notes and Other Information
- This is a static inline function defined in src/common/hmac_openssl.c
- Part of PostgreSQL's resource management system for HMAC contexts
- Paired with ResourceOwnerForgetHMAC for complete resource lifecycle management
- Uses the hmac_resowner_desc descriptor to specify cleanup behavior