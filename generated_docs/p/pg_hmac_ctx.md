# pg_hmac_ctx

## Location
src/common/hmac_openssl.c: 64 - 90

## Overview
A structure that represents the internal context for HMAC (Hash-based Message Authentication Code) operations in PostgreSQL, providing a unified interface for computing HMACs using different cryptographic hash algorithms.

## Definition
```c
struct pg_hmac_ctx
{
    pg_cryptohash_ctx *hash;
    pg_cryptohash_type type;
    pg_hmac_errno error;
    const char *errreason;
    int         block_size;
    int         digest_size;

    /*
     * Use the largest block size among supported options.  This wastes some
     * memory but simplifies the allocation logic.
     */
    uint8       k_ipad[PG_SHA512_BLOCK_LENGTH];
    uint8       k_opad[PG_SHA512_BLOCK_LENGTH];
};
```

## Detailed Description
The `pg_hmac_ctx` structure encapsulates all necessary state for performing HMAC computations. It wraps an underlying cryptographic hash context (`pg_cryptohash_ctx`) and maintains the necessary padding keys (k_ipad and k_opad) required by the HMAC algorithm. The structure is designed to support multiple hash algorithms (SHA1, SHA224, SHA256, SHA384, SHA512) while providing a consistent interface.

The HMAC algorithm requires two derived keys from the original key: an inner padding key (k_ipad) and outer padding key (k_opad). These are pre-computed and stored in the context to avoid recalculation during update operations. The structure uses fixed-size arrays sized for the largest supported block length (SHA512) to simplify memory management, though this may waste some memory for smaller hash algorithms.

## Parameters / Member Variables
- `hash`: Pointer to the underlying cryptographic hash context that performs the actual hash computations
- `type`: The type of cryptographic hash algorithm being used (e.g., SHA1, SHA256, SHA512)
- `error`: Error code indicating the last error that occurred during HMAC operations
- `errreason`: Human-readable description of the last error
- `block_size`: The block size in bytes for the current hash algorithm
- `digest_size`: The output digest size in bytes for the current hash algorithm
- `k_ipad`: Inner padding key derived from the original HMAC key, XORed with 0x36
- `k_opad`: Outer padding key derived from the original HMAC key, XORed with 0x5C

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md)
  - pg_cryptohash_type
  - pg_hmac_errno
  - PG_SHA512_BLOCK_LENGTH

- Called from (representative examples):
  - [pg_hmac_create](pg_hmac_create.md)
  - [pg_hmac_init](pg_hmac_init.md)
  - [pg_hmac_update](pg_hmac_update.md)
  - [pg_hmac_final](pg_hmac_final.md)
  - [pg_hmac_free](pg_hmac_free.md)
  - [pg_hmac_error](pg_hmac_error.md)
  - [verify_client_proof](../v/verify_client_proof.md) (SCRAM authentication)
  - [build_server_final_message](../b/build_server_final_message.md) (SCRAM authentication)
  - scram_SaltedPassword
  - [calculate_client_proof](../c/calculate_client_proof.md)

## Notes and Other Information
- This structure is internal to the HMAC implementation and should not be accessed directly by client code
- The fixed-size k_ipad and k_opad arrays are sized for SHA512 block length to accommodate all supported hash algorithms
- Used extensively in PostgreSQL's SCRAM-SHA-256 authentication implementation
- The structure supports both OpenSSL and built-in cryptographic implementations
- Memory allocation is simplified by using the largest possible block size, trading memory efficiency for implementation simplicity
- Error handling is built into the structure with both error codes and human-readable error descriptions