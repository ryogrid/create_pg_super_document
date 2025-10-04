# verify_server_signature

## Location
[src/interfaces/libpq/fe-auth-scram.c:830-891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L830-L891)

## Overview
Validates the server signature received as part of the final exchange message from the server during SCRAM authentication to ensure server authenticity.

## Definition

```c
static bool
verify_server_signature(fe_scram_state *state, bool *match,
						const char **errstr)
```
## Detailed Description
This function implements server signature verification as part of the SCRAM (Salted Challenge Response Authentication Mechanism) protocol in PostgreSQL's libpq client library. It calculates the expected server signature using the stored authentication state and compares it with the signature received from the server. This verification step is crucial for mutual authentication, ensuring that the server possesses the correct authentication credentials and preventing man-in-the-middle attacks.

The function computes the ServerSignature by first deriving the ServerKey from the SaltedPassword, then creating an HMAC of the concatenated authentication messages using this ServerKey. The calculated signature is then compared with the server-provided signature to determine authenticity.

## Parameters / Member Variables
- `*state`: Pointer to fe_scram_state structure containing SCRAM authentication state including SaltedPassword, hash type, key length, and authentication messages
- `*match`: Output parameter set to true if server signature matches expected value, false otherwise
- `**errstr`: Output parameter pointing to error message string if processing fails
## Dependencies
- Functions called/Symbols referenced:
  - [pg_hmac_create](../p/pg_hmac_create.md)
  - [pg_hmac_error](../p/pg_hmac_error.md)
  - [scram_ServerKey](../s/scram_ServerKey.md)
  - [pg_hmac_free](../p/pg_hmac_free.md)
  - [pg_hmac_init](../p/pg_hmac_init.md)
  - [pg_hmac_update](../p/pg_hmac_update.md)
  - [pg_hmac_final](../p/pg_hmac_final.md)
  - memcmp
- Called from (representative examples):
  - [scram_exchange](../s/scram_exchange.md)

## Notes and Other Information
- This is a static function internal to fe-auth-scram.c
- Returns true for successful processing (regardless of signature match), false for processing errors
- The actual match result is returned via the match parameter
- Uses HMAC with the hash type specified in the SCRAM state (typically SHA-256)
- Part of the SCRAM authentication protocol implementation in PostgreSQL's client library
- Critical for preventing authentication bypass and man-in-the-middle attacks
- Requires proper cleanup of HMAC context on both success and failure paths

## Simplified Source

```c
static bool verify_server_signature(fe_scram_state *state, bool *match, const char **errstr) {
    uint8 expected_ServerSignature[SCRAM_MAX_KEY_LEN];
    uint8 ServerKey[SCRAM_MAX_KEY_LEN];
    pg_hmac_ctx *ctx;

    // Create HMAC context
    ctx = pg_hmac_create(state->hash_type);
    if (ctx == NULL) {
        *errstr = pg_hmac_error(NULL);
        return false;
    }

    // Derive ServerKey from SaltedPassword
    if (scram_ServerKey(state->SaltedPassword, state->hash_type,
                        state->key_length, ServerKey, errstr) < 0) {
        pg_hmac_free(ctx);
        return false;
    }

    // Calculate expected ServerSignature using HMAC
    // HMAC(ServerKey, client_first_bare + "," + server_first + "," + client_final_without_proof)
    if (pg_hmac_init(ctx, ServerKey, state->key_length) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->client_first_message_bare,
                       strlen(state->client_first_message_bare)) < 0 ||
        pg_hmac_update(ctx, (uint8 *) ",", 1) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->server_first_message,
                       strlen(state->server_first_message)) < 0 ||
        pg_hmac_update(ctx, (uint8 *) ",", 1) < 0 ||
        pg_hmac_update(ctx, (uint8 *) state->client_final_message_without_proof,
                       strlen(state->client_final_message_without_proof)) < 0 ||
        pg_hmac_final(ctx, expected_ServerSignature, state->key_length) < 0) {
        *errstr = pg_hmac_error(ctx);
        pg_hmac_free(ctx);
        return false;
    }

    pg_hmac_free(ctx);

    // Compare calculated signature with server's signature
    *match = (memcmp(expected_ServerSignature, state->ServerSignature, state->key_length) == 0);

    return true;
}
```