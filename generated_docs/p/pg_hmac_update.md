# pg_hmac_update

## Location
[src/common/hmac_openssl.c:229-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L229-L253)

## Overview
Updates a HMAC context with new data during the HMAC computation process.

## Definition

```c
int
pg_hmac_update(pg_hmac_ctx *ctx, const uint8 *data, size_t len)
```
## Detailed Description
The pg_hmac_update function feeds data into an existing HMAC context for incremental HMAC computation. This function allows processing data in chunks rather than requiring all data to be available at once. It serves as a wrapper around the underlying cryptographic hash update function, providing error handling specific to HMAC operations. The function returns 0 on success and -1 on failure, setting appropriate error codes in the context.

## Parameters / Member Variables
- : Pointer to the HMAC context structure that maintains the computation state
- : Pointer to the data buffer to be processed
- : Size of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_update](pg_cryptohash_update.md)
  - [pg_cryptohash_error](pg_cryptohash_error.md)
  - PG_HMAC_ERROR_INTERNAL
- Called from (representative examples):
  - [verify_client_proof](../v/verify_client_proof.md) (SCRAM authentication)
  - [build_server_final_message](../b/build_server_final_message.md) (SCRAM authentication)
  - [scram_SaltedPassword](../s/scram_SaltedPassword.md) (SCRAM key derivation)
  - [calculate_client_proof](../c/calculate_client_proof.md) (libpq SCRAM client)

## Notes and Other Information
- Part of PostgreSQL's HMAC implementation used primarily for SCRAM authentication
- Must be called after pg_hmac_init and before pg_hmac_final
- Can be called multiple times to process data incrementally
- Sets ctx->error to PG_HMAC_ERROR_INTERNAL and ctx->errreason on failure
- Validates that the context pointer is not NULL before processing