# scram_ServerKey

## Location
src/common/scram-common.c: 172 - 208

## Overview
Computes the ServerKey component used in SCRAM authentication by applying HMAC to the salted password with the "Server Key" string.

## Definition
```c
int scram_ServerKey(const uint8 *salted_password,
                   pg_cryptohash_type hash_type, int key_length,
                   uint8 *result, const char **errstr)
```

## Detailed Description
This function calculates the ServerKey as defined in the SCRAM specification by computing HMAC(SaltedPassword, "Server Key"). The ServerKey is used by the server to generate the server signature during SCRAM authentication, which allows the client to verify that the server possesses the correct password information without the server actually knowing the plaintext password. The function uses the provided salted password as the HMAC key and the literal string "Server Key" as the message to authenticate.

## Parameters / Member Variables
- `salted_password`: The salted password bytes to use as the HMAC key
- `hash_type`: The cryptographic hash algorithm to use in HMAC (e.g., SHA-256)
- `key_length`: The length of the salted password and expected result in bytes
- `result`: Output buffer to store the computed ServerKey
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
  - scram_verify_plain_password
  - scram_build_secret
  - verify_server_signature

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses the literal string "Server Key" as defined in SCRAM specification (RFC 5802)
- The resulting ServerKey is used to generate the server signature for mutual authentication
- Part of the SCRAM key derivation chain: Password → SaltedPassword → ServerKey → ServerSignature
- Complements scram_ClientKey function, both deriving different keys from the same salted password
- Handles memory management automatically with proper cleanup via pg_hmac_free()
- Thread-safe as it uses local variables and doesn't modify global state