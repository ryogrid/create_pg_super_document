# pg_hmac_update

## Location
src/common/hmac_openssl.c: 229 - 253

## Overview
Updates a HMAC context with new data during the HMAC computation process.

## Definition


## Detailed Description
The pg_hmac_update function feeds data into an existing HMAC context for incremental HMAC computation. This function allows processing data in chunks rather than requiring all data to be available at once. It serves as a wrapper around the underlying cryptographic hash update function, providing error handling specific to HMAC operations. The function returns 0 on success and -1 on failure, setting appropriate error codes in the context.

## Parameters / Member Variables
- : Pointer to the HMAC context structure that maintains the computation state
- : Pointer to the data buffer to be processed
- : Size of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_cryptohash_update
  - pg_cryptohash_error
  - PG_HMAC_ERROR_INTERNAL
- Called from (representative examples):
  - verify_client_proof (SCRAM authentication)
  - build_server_final_message (SCRAM authentication)
  - scram_SaltedPassword (SCRAM key derivation)
  - calculate_client_proof (libpq SCRAM client)

## Notes and Other Information
- Part of PostgreSQL's HMAC implementation used primarily for SCRAM authentication
- Must be called after pg_hmac_init and before pg_hmac_final
- Can be called multiple times to process data incrementally
- Sets ctx->error to PG_HMAC_ERROR_INTERNAL and ctx->errreason on failure
- Validates that the context pointer is not NULL before processing