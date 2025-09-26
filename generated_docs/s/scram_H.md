# scram_H

## Location
[src/common/scram-common.c:112-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/scram-common.c#L112-L141)

## Overview
Calculates a cryptographic hash of input data using a specified hash algorithm, primarily used in SCRAM authentication for hashing keys and passwords.

## Definition
```c
int scram_H(const uint8 *input, pg_cryptohash_type hash_type, int key_length,
           uint8 *result, const char **errstr)
```

## Detailed Description
This function performs a straightforward cryptographic hash operation on input data using the specified hash algorithm (typically SHA-256 in SCRAM contexts). It creates a hash context, processes the input data, and produces the hash result. The function is a wrapper around PostgreSQL's cryptographic hash API, providing error handling and cleanup. Unlike HMAC operations, this performs a simple hash without any secret key.

## Parameters / Member Variables
- `input`: Pointer to the input data to be hashed
- `hash_type`: The cryptographic hash algorithm to use (e.g., SHA-256)
- `key_length`: The length of the input data in bytes
- `result`: Output buffer to store the hash result
- `errstr`: Pointer to error message string on failure

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_create](../p/pg_cryptohash_create.md)
  - [pg_cryptohash_init](../p/pg_cryptohash_init.md)
  - [pg_cryptohash_update](../p/pg_cryptohash_update.md)
  - [pg_cryptohash_final](../p/pg_cryptohash_final.md)
  - [pg_cryptohash_error](../p/pg_cryptohash_error.md)
  - [pg_cryptohash_free](../p/pg_cryptohash_free.md)
- Called from (representative examples):
  - [verify_client_proof](../v/verify_client_proof.md)
  - [scram_build_secret](scram_build_secret.md)
  - [calculate_client_proof](../c/calculate_client_proof.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Despite the comment mentioning NULL-terminated strings, the function actually hashes exactly `key_length` bytes of input data
- Used in SCRAM authentication to compute H(ClientKey) and H(ServerKey) operations
- Handles memory management automatically with proper cleanup via pg_cryptohash_free()
- Thread-safe as it uses local variables and doesn't modify global state
- The function name 'H' follows SCRAM specification notation where H() represents the hash function