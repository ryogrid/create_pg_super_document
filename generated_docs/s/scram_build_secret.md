# scram_build_secret

## Location
src/common/scram-common.c: 209 - 329

## Overview
Constructs a complete SCRAM secret string suitable for storage in pg_authid.rolpassword by performing all necessary SCRAM key derivations and encoding the result.

## Definition
```c
char *scram_build_secret(pg_cryptohash_type hash_type, int key_length,
                        const char *salt, int saltlen, int iterations,
                        const char *password, const char **errstr)
```

## Detailed Description
This function is the main entry point for creating SCRAM authentication secrets that can be stored in the database. It performs the complete SCRAM key derivation process: derives the SaltedPassword using PBKDF2, calculates the ClientKey and ServerKey, computes the StoredKey by hashing the ClientKey, and then formats everything into a standardized string format. The output format follows the pattern "SCRAM-SHA-256$<iterations>:<base64_salt>$<base64_stored_key>:<base64_server_key>". The function handles memory allocation differently for frontend vs backend builds.

## Parameters / Member Variables
- `hash_type`: The cryptographic hash algorithm to use (currently only PG_SHA256 supported)
- `key_length`: The length of cryptographic keys in bytes
- `salt`: The random salt bytes to use in key derivation
- `saltlen`: The length of the salt in bytes
- `iterations`: The number of PBKDF2 iterations to perform
- `password`: The plaintext password (should be SASLprep normalized)
- `errstr`: Pointer to error message string on failure

## Dependencies
- Functions called/Symbols referenced:
  - scram_SaltedPassword
  - scram_ClientKey  
  - scram_H
  - scram_ServerKey
  - pg_b64_encode
  - pg_b64_enc_len
  - malloc/palloc (build-dependent)
- Called from (representative examples):
  - pg_be_scram_build_secret
  - pg_fe_scram_build_secret

## Notes and Other Information
- Returns allocated string on success, NULL on failure
- Caller is responsible for freeing the returned string (malloc/palloc depending on build)
- Currently only supports SHA-256 hash algorithm (asserts PG_SHA256)
- Requires positive iteration count (asserts iterations > 0)
- Output string format: "SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>" with Base64 encoding
- Uses different memory allocation strategies: malloc() in frontend, palloc() in backend
- Implements complete SCRAM key derivation chain in a single function call
- Password should be pre-processed with SASLprep normalization before calling