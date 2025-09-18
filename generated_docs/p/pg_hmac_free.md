# pg_hmac_free

## Location
[src/common/hmac_openssl.c:326-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L326-L353)

## Overview
Frees a HMAC context and securely clears its memory contents.

## Definition
```c
void pg_hmac_free(pg_hmac_ctx *ctx)
```

## Detailed Description
The pg_hmac_free function properly deallocates a HMAC context structure and ensures that sensitive data is securely erased from memory. It first frees the underlying cryptographic hash context, then uses explicit_bzero to securely clear the entire context structure before freeing the memory. This prevents sensitive key material and intermediate computation states from remaining in memory after the context is no longer needed.

## Parameters / Member Variables
- `ctx`: Pointer to the HMAC context structure to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_free](pg_cryptohash_free.md)
  - explicit_bzero
  - FREE (memory deallocation)
- Called from (representative examples):
  - [verify_client_proof](../v/verify_client_proof.md) (SCRAM authentication cleanup)
  - [build_server_final_message](../b/build_server_final_message.md) (SCRAM authentication cleanup)
  - [ResOwnerReleaseHMAC](../R/ResOwnerReleaseHMAC.md) (resource owner cleanup)
  - scram_SaltedPassword (SCRAM key derivation cleanup)
  - [calculate_client_proof](../c/calculate_client_proof.md) (libpq SCRAM client cleanup)

## Notes and Other Information
- Safe to call with NULL pointer (performs null check)
- Uses explicit_bzero for secure memory clearing to prevent sensitive data leakage
- Should be called after completing HMAC operations to free resources
- Part of proper resource management for HMAC contexts
- Essential for security to prevent key material from remaining in memory
- Frees both the internal hash context and the HMAC context itself