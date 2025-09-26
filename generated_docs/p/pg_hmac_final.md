# pg_hmac_final

## Location
[src/common/hmac_openssl.c:254-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L254-L325)

## Overview
Finalizes a HMAC computation and produces the final HMAC value.

## Definition
```c
int pg_hmac_final(pg_hmac_ctx *ctx, uint8 *dest, size_t len)
```

## Detailed Description
The pg_hmac_final function completes the HMAC computation process by implementing the second phase of the HMAC algorithm. It first finalizes the inner hash computation, then performs the outer hash computation using the outer padding key (k_opad) and the result of the inner hash. This function allocates temporary memory for the intermediate hash result and ensures proper cleanup. The function returns 0 on success and -1 on failure, with detailed error information stored in the context.

## Parameters / Member Variables
- `ctx`: Pointer to the HMAC context structure containing the computation state
- `dest`: Output buffer where the final HMAC value will be stored
- `len`: Size of the output buffer, should match the expected digest size

## Dependencies
- Functions called/Symbols referenced:
  - ALLOC (memory allocation)
  - FREE (memory deallocation)
  - [pg_cryptohash_final](pg_cryptohash_final.md)
  - [pg_cryptohash_init](pg_cryptohash_init.md)
  - [pg_cryptohash_update](pg_cryptohash_update.md)
  - [pg_cryptohash_error](pg_cryptohash_error.md)
  - PG_HMAC_ERROR_OOM
  - PG_HMAC_ERROR_INTERNAL
- Called from (representative examples):
  - [verify_client_proof](../v/verify_client_proof.md) (SCRAM authentication)
  - [build_server_final_message](../b/build_server_final_message.md) (SCRAM authentication)
  - [scram_SaltedPassword](../s/scram_SaltedPassword.md) (SCRAM key derivation)
  - [calculate_client_proof](../c/calculate_client_proof.md) (libpq SCRAM client)

## Notes and Other Information
- Implements the final step of HMAC: H(K XOR opad, H(K XOR ipad, text))
- Must be called after pg_hmac_init and any number of pg_hmac_update calls
- Allocates temporary memory for intermediate hash results and ensures cleanup
- Sets appropriate error codes (PG_HMAC_ERROR_OOM for memory allocation failures)
- Uses the outer padding key (k_opad) stored in the context during initialization
- After calling this function, the context should be freed with pg_hmac_free