# scram_mock_salt

## Location
[src/backend/libpq/auth-scram.c:1458-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L1458-L1492)

## Overview
Deterministically generates a salt for mock SCRAM authentication using a SHA256 hash based on the username and a cluster-level secret key to prevent timing attacks during failed authentication attempts.

## Definition

```c
static char *
scram_mock_salt(const char *username, pg_cryptohash_type hash_type,
				int key_length)
```
## Detailed Description
This function is a crucial security component that generates deterministic salt values for mock SCRAM authentication. It serves as a defense against timing attacks by ensuring that failed authentication attempts take approximately the same amount of time as successful ones.

The function creates a salt by computing a SHA256 hash of two inputs:
1. **Username**: The provided username attempting authentication
2. **Mock Authentication Nonce**: A cluster-level secret obtained from GetMockAuthenticationNonce()

The deterministic nature ensures that the same username will always produce the same mock salt, while the cluster-level secret ensures that different PostgreSQL clusters will generate different mock salts for the same username. This prevents attackers from pre-computing salts across different installations.

The function includes compile-time assertions to ensure that the SHA256 digest length is sufficient for the default SCRAM salt length requirements.

## Parameters / Member Variables
- `*username`: The username for which to generate a mock salt (used as hash input)
- `hash_type`: The cryptographic hash algorithm type (currently only PG_SHA256 is supported)
- `key_length`: The required length for the generated salt (must not exceed SCRAM_MAX_KEY_LEN)
## Dependencies
- Functions called/Symbols referenced:
  - : Retrieve cluster-level secret nonce for salt generation
  - : Create cryptographic hash context
  - : Initialize hash context
  - : Add data to hash computation
  - : Finalize hash and retrieve result
  - : Free hash context resources
  - : Compile-time assertion macro
  - Constants: , , , , 
- Called from (representative examples):
  - : Generate complete mock SCRAM authentication data

## Notes and Other Information
- **Security Purpose**: Prevents timing attacks by ensuring consistent execution time for both valid and invalid authentication attempts
- **Deterministic Generation**: Same username always produces the same salt within a cluster, enabling consistent mock authentication behavior
- **Cluster Isolation**: Different PostgreSQL clusters generate different mock salts for the same username due to unique cluster-level nonces
- **Static Buffer**: Returns pointer to static buffer, so the returned value is only valid until the next call to this function
- **Hash Algorithm Limitation**: Currently only supports SHA256, as indicated by the assertion
- **Memory Management**: Uses static buffer to avoid memory allocation overhead during authentication
- **Compile-time Safety**: Includes static assertions to ensure buffer sizes are adequate for the salt requirements
- **Error Handling**: Returns NULL on cryptographic operation failure, allowing caller to handle errors appropriately