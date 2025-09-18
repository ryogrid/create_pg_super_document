# pg_md5_binary

## Location
[src/common/md5_common.c:108-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5_common.c#L108-L144)

## Overview
Calculates the MD5 hash digest of a buffer and returns it as a binary byte array rather than hexadecimal string representation.

## Definition


## Detailed Description
This function computes the MD5 cryptographic hash of the specified input buffer using PostgreSQL's cryptographic hash framework, similar to pg_md5_hash, but returns the result as raw binary data instead of converting it to a hexadecimal string. The function outputs the 16-byte MD5 digest directly to the provided output buffer, making it suitable for applications that need the binary hash value for further processing or storage.

Like pg_md5_hash, this function uses PostgreSQL's cryptohash API for secure MD5 computation with proper error handling and memory management, but skips the hexadecimal conversion step to provide the raw digest bytes.

## Parameters / Member Variables
- : Pointer to the input buffer containing the data to be hashed
- : Size in bytes of the input buffer
- : Output buffer to receive the 16-byte binary MD5 digest (must be at least MD5_DIGEST_LENGTH bytes)
- : Pointer to a const char pointer that will be set to an error message on failure, or NULL on success

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_create](pg_cryptohash_create.md)
  - [pg_cryptohash_init](pg_cryptohash_init.md)
  - [pg_cryptohash_update](pg_cryptohash_update.md)
  - [pg_cryptohash_final](pg_cryptohash_final.md)
  - [pg_cryptohash_error](pg_cryptohash_error.md)
  - [pg_cryptohash_free](pg_cryptohash_free.md)
  - MD5_DIGEST_LENGTH
  - PG_MD5
  - [pg_cryptohash_ctx](pg_cryptohash_ctx.md)
- Called from (representative examples):
  - [PerformRadiusTransaction](../P/PerformRadiusTransaction.md)

## Notes and Other Information
- Returns true on success, false on failure
- The output buffer must be pre-allocated with at least MD5_DIGEST_LENGTH (16) bytes of space
- More efficient than pg_md5_hash when binary output is preferred, as it avoids the hexadecimal conversion overhead
- Uses the same cryptographic framework as pg_md5_hash but returns raw binary digest
- Provides detailed error reporting through the errstr parameter
- Handles memory management automatically, including cleanup on both success and failure paths
- Commonly used in authentication protocols (like RADIUS) where binary hash values are required
- Error conditions include out-of-memory scenarios and MD5 computation failures