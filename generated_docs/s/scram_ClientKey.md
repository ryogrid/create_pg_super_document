# scram_ClientKey

## Location
src/common/scram-common.c: 142 - 171

## Overview
Computes the ClientKey component used in SCRAM authentication by applying HMAC to the salted password with the "Client Key" string.

## Definition
```c
int scram_ClientKey(const uint8 *salted_password,
                   pg_cryptohash_type hash_type, int key_length,
                   uint8 *result, const char **errstr)
```

## Detailed Description
This function calculates the ClientKey as defined in the SCRAM specification by computing HMAC(SaltedPassword, "Client Key"). The ClientKey is a crucial component in SCRAM authentication, used to derive the StoredKey (through hashing) and to generate the client proof during authentication. The function uses the provided salted password as the HMAC key and the literal string "Client Key" as the message to authenticate.

## Parameters / Member Variables
- `salted_password`: The salted password bytes to use as the HMAC key
- `hash_type`: The cryptographic hash algorithm to use in HMAC (e.g., SHA-256)
- `key_length`: The length of the salted password and expected result in bytes
- `result`: Output buffer to store the computed ClientKey
- `errstr`: Pointer to error message string on failure

## Dependencies
- Functions called/Symbols referenced:
  - pg_hmac_create
  - pg_hmac_init
  - pg_hmac_update
  - pg_hmac_final
  - pg_hmac_error
  - pg_hmac_free
- Called from (representative examples):
  - scram_build_secret
  - calculate_client_proof

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses the literal string "Client Key" as defined in SCRAM specification (RFC 5802)
- The resulting ClientKey is typically hashed with scram_H() to produce the StoredKey
- Part of the SCRAM key derivation chain: Password → SaltedPassword → ClientKey → StoredKey
- Handles memory management automatically with proper cleanup via pg_hmac_free()
- Thread-safe as it uses local variables and doesn't modify global state