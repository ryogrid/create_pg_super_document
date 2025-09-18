# pg_md5_hash

## Location
[src/common/md5_common.c:74-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5_common.c#L74-L107)

## Overview
Calculates the MD5 hash digest of a buffer and returns it as a hexadecimal string representation.

## Definition


## Detailed Description
This function computes the MD5 cryptographic hash of the specified input buffer using PostgreSQL's cryptographic hash framework. It processes the input data through the MD5 algorithm and converts the resulting 16-byte binary digest into a 32-character lowercase hexadecimal string representation. The function provides comprehensive error handling and memory management for the cryptographic operations.

The implementation uses PostgreSQL's cryptohash API for secure MD5 computation, ensuring proper initialization, data processing, finalization, and cleanup of cryptographic contexts. Upon successful computation, it converts the binary hash to hexadecimal format using the bytesToHex utility function.

## Parameters / Member Variables
- : Pointer to the input buffer containing the data to be hashed
- : Size in bytes of the input buffer
- : Output buffer to receive the 32-character hexadecimal hash string plus null terminator (must be at least 33 bytes)
- : Pointer to a const char pointer that will be set to an error message on failure, or NULL on success

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_create](pg_cryptohash_create.md)
  - [pg_cryptohash_init](pg_cryptohash_init.md)
  - [pg_cryptohash_update](pg_cryptohash_update.md)
  - [pg_cryptohash_final](pg_cryptohash_final.md)
  - [pg_cryptohash_error](pg_cryptohash_error.md)
  - [pg_cryptohash_free](pg_cryptohash_free.md)
  - [bytesToHex](../b/bytesToHex.md)
  - MD5_DIGEST_LENGTH
  - PG_MD5
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md)
- Called from (representative examples):
  - [md5_text](../m/md5_text.md)
  - [md5_bytea](../m/md5_bytea.md)
  - [pg_md5_encrypt](pg_md5_encrypt.md)

## Notes and Other Information
- Returns true on success, false on failure
- Follows RFC 1321 standards for MD5 computation
- Provides detailed error reporting through the errstr parameter
- Handles memory management automatically, including cleanup on both success and failure paths
- The output hexsum buffer must be pre-allocated with at least 33 bytes of space
- Uses PostgreSQL's internal cryptographic hash framework for secure and consistent hash computation
- Originally authored by Sverre H. Huseby <sverrehu@online.no>
- Error conditions include out-of-memory scenarios and MD5 computation failures