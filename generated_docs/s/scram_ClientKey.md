# scram_ClientKey

## Location
[src/common/scram-common.c:142-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/scram-common.c#L142-L171)

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
  - [pg_hmac_create](../p/pg_hmac_create.md)
  - [pg_hmac_init](../p/pg_hmac_init.md)
  - [pg_hmac_update](../p/pg_hmac_update.md)
  - [pg_hmac_final](../p/pg_hmac_final.md)
  - [pg_hmac_error](../p/pg_hmac_error.md)
  - [pg_hmac_free](../p/pg_hmac_free.md)
- Called from (representative examples):
  - [scram_build_secret](scram_build_secret.md)
  - [calculate_client_proof](../c/calculate_client_proof.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses the literal string "Client Key" as defined in SCRAM specification (RFC 5802)
- The resulting ClientKey is typically hashed with scram_H() to produce the StoredKey
- Part of the SCRAM key derivation chain: Password → SaltedPassword → ClientKey → StoredKey
- Handles memory management automatically with proper cleanup via pg_hmac_free()
- Thread-safe as it uses local variables and doesn't modify global state

## Simplified Source

```c
int scram_ClientKey(const uint8 *salted_password,
                    pg_cryptohash_type hash_type, int key_length,
                    uint8 *result, const char **errstr) {
    // Create HMAC context
    pg_hmac_ctx *ctx = pg_hmac_create(hash_type);
    if (ctx == NULL) {
        *errstr = pg_hmac_error(NULL);
        return -1;
    }

    // Compute HMAC(SaltedPassword, "Client Key")
    if (pg_hmac_init(ctx, salted_password, key_length) < 0 ||
        pg_hmac_update(ctx, (uint8 *) "Client Key", strlen("Client Key")) < 0 ||
        pg_hmac_final(ctx, result, key_length) < 0) {
        *errstr = pg_hmac_error(ctx);
        pg_hmac_free(ctx);
        return -1;
    }

    pg_hmac_free(ctx);
    return 0;
}
```