# mock_scram_secret

## Location
[src/backend/libpq/auth-scram.c:683-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L683-L728)

## Overview
Generates plausible SCRAM secret parameters for mock authentication when there is no stored secret available in the server.

## Definition


## Detailed Description
This function creates realistic-looking SCRAM authentication parameters for use in mock authentication scenarios. It is called when a user attempts to authenticate but no valid stored secret exists, helping to prevent timing attacks by ensuring the authentication process takes roughly the same amount of time regardless of whether a user exists or not.

The function generates deterministic salt based on the username using the cluster's nonce value, sets standard SCRAM-SHA-256 parameters, and zeros out the stored and server keys since they won't be used in the doomed authentication process. All generated parameters appear realistic to prevent information disclosure to potential attackers.

## Parameters / Member Variables
- : The username for which to generate mock parameters (used for deterministic salt generation)
- : Output parameter set to PG_SHA256 (enforced for consistency)
- : Output parameter set to SCRAM_SHA_256_DEFAULT_ITERATIONS 
- : Output parameter set to SCRAM_SHA_256_KEY_LEN
- : Output parameter for the base64-encoded salt string (palloc'd)
- : Pre-allocated buffer that is zeroed out (not used in mock authentication)
- : Pre-allocated buffer that is zeroed out (not used in mock authentication)

## Dependencies
- Functions called/Symbols referenced:
  - [scram_mock_salt](../s/scram_mock_salt.md)
  - pg_b64_enc_len
  - pg_b64_encode
  - [palloc](../p/palloc.md)
  - memset
  - elog
  - PG_SHA256
  - SCRAM_SHA_256_KEY_LEN
  - SCRAM_DEFAULT_SALT_LEN
  - SCRAM_SHA_256_DEFAULT_ITERATIONS
  - SCRAM_MAX_KEY_LEN
- Called from (representative examples):
  - [scram_init](../s/scram_init.md)

## Notes and Other Information
- This is a static function, only accessible within auth-scram.c
- Part of PostgreSQL's defense against timing attacks during authentication
- Always uses SCRAM-SHA-256 algorithm for consistency
- Error messages are kept generic to avoid information disclosure
- The stored_key and server_key are intentionally zeroed since mock authentication will always fail
- Uses deterministic salt generation to ensure consistent behavior across multiple authentication attempts with the same username